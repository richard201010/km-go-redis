// Package acl 实现 Redis ACL 访问控制 (Access Control List) support.
// This is the Go equivalent of Redis's acl.c.
//
// ACL provides fine-grained access control:
// - User management (CREATE, SETUSER, DELUSER, LIST, WHOAMI)
// - Password authentication (plaintext and hashed)
// - Command permissions (on/off per command or category)
// - Key pattern permissions (~pattern, %pattern)
package acl

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
)

// ACLUser represents a Redis ACL user.
type ACLUser struct {
	Name         string
	Passwords    []string  // hashed passwords
	Enabled      bool
	Flags        int
	Commands     map[string]bool // command -> allowed
	Categories   map[string]bool // @category -> allowed/disallowed
	KeyPatterns  []string        // ~pattern for key access
	ChannelPatterns []string     // &pattern for pub/sub channels
	SelectorRules []string
}

// User flags
const (
	UserFlagEnabled   = 1 << 0
	UserFlagAllKeys   = 1 << 1
	UserFlagAllChannels = 1 << 2
)

// ACL 管理用户和访问控制.
type ACL struct {
	mu    sync.RWMutex
	Users map[string]*ACLUser
	DefaultUser *ACLUser
	Enabled bool
}

// NewACL creates a new ACL manager.
func NewACL() *ACL {
	acl := &ACL{
		Users:   make(map[string]*ACLUser),
		Enabled: false,
	}
	// Create default user (like Redis's "default" user)
	defaultUser := &ACLUser{
		Name:      "default",
		Enabled:   true,
		Flags:     UserFlagEnabled | UserFlagAllKeys | UserFlagAllChannels,
		Commands:  make(map[string]bool),
		Categories: map[string]bool{
			"@all":      true,
			"@admin":    true,
			"@dangerous": true,
		},
		KeyPatterns:     []string{"*"},
		ChannelPatterns: []string{"*"},
	}
	acl.DefaultUser = defaultUser
	acl.Users["default"] = defaultUser
	return acl
}

// Authenticate 检查用户是否可以认证 with the given password.
func (a *ACL) Authenticate(username, password string) (*ACLUser, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if username == "" {
		username = "default"
	}
	user, ok := a.Users[username]
	if !ok {
		return nil, fmt.Errorf("WRONGPASS invalid username-password pair or user disabled.")
	}
	if !user.Enabled {
		return nil, fmt.Errorf("WRONGPASS invalid username-password pair or user disabled.")
	}
	if len(user.Passwords) == 0 {
		// No password required
		return user, nil
	}
	hashed := hashPassword(password)
	for _, p := range user.Passwords {
		if p == hashed {
			return user, nil
		}
	}
	return nil, fmt.Errorf("WRONGPASS invalid username-password pair or user disabled.")
}

// CheckCommand 检查用户是否有权限执行命令 to execute a command.
func (a *ACL) CheckCommand(user *ACLUser, cmdName string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.Enabled {
		return true
	}
	if user == nil {
		user = a.DefaultUser
	}
	// Check specific command
	if allowed, ok := user.Commands[cmdName]; ok {
		return allowed
	}
	// Check categories
	// If user has @all, allow everything except explicitly denied
	if user.Categories["@all"] {
		// Check if explicitly denied
		if denied, ok := user.Commands["-"+cmdName]; ok && denied {
			return false
		}
		return true
	}
	return false
}

// CheckKey checks if a user can access a key matching the pattern.
func (a *ACL) CheckKey(user *ACLUser, key string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.Enabled {
		return true
	}
	if user == nil {
		user = a.DefaultUser
	}
	if user.Flags&UserFlagAllKeys != 0 {
		return true
	}
	for _, pattern := range user.KeyPatterns {
		if matchPattern(pattern, key) {
			return true
		}
	}
	return false
}

// CheckChannel checks if a user can access a pub/sub channel.
func (a *ACL) CheckChannel(user *ACLUser, channel string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.Enabled {
		return true
	}
	if user == nil {
		user = a.DefaultUser
	}
	if user.Flags&UserFlagAllChannels != 0 {
		return true
	}
	for _, pattern := range user.ChannelPatterns {
		if matchPattern(pattern, channel) {
			return true
		}
	}
	return false
}

// CreateUser 创建新 ACL 用户.
func (a *ACL) CreateUser(name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.Users[name]; exists {
		return fmt.Errorf("ERR user '%s' already exists", name)
	}
	a.Users[name] = &ACLUser{
		Name:      name,
		Enabled:   false,
		Commands:  make(map[string]bool),
		Categories: make(map[string]bool),
	}
	return nil
}

// SetUser 配置 ACL 用户.
func (a *ACL) SetUser(name string, rules []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	user, ok := a.Users[name]
	if !ok {
		// Auto-create
		user = &ACLUser{
			Name:      name,
			Commands:  make(map[string]bool),
			Categories: make(map[string]bool),
		}
		a.Users[name] = user
	}
	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" {
			continue
		}
		switch {
		case rule == "on":
			user.Enabled = true
			user.Flags |= UserFlagEnabled
		case rule == "off":
			user.Enabled = false
			user.Flags &^= UserFlagEnabled
		case strings.HasPrefix(rule, ">"):
			// Add password
			hashed := hashPassword(rule[1:])
			user.Passwords = append(user.Passwords, hashed)
		case strings.HasPrefix(rule, "#"):
			// Add hashed password
			user.Passwords = append(user.Passwords, rule[1:])
		case strings.HasPrefix(rule, "!"):
			// Remove password
			hashed := hashPassword(rule[1:])
			newPwds := make([]string, 0, len(user.Passwords))
			for _, p := range user.Passwords {
				if p != hashed {
					newPwds = append(newPwds, p)
				}
			}
			user.Passwords = newPwds
		case rule == "nopass":
			user.Passwords = nil
		case strings.HasPrefix(rule, "~"):
			// Key pattern
			pattern := rule[1:]
			if pattern == "*" {
				user.Flags |= UserFlagAllKeys
			}
			user.KeyPatterns = append(user.KeyPatterns, pattern)
		case strings.HasPrefix(rule, "&"):
			// Channel pattern
			pattern := rule[1:]
			if pattern == "*" {
				user.Flags |= UserFlagAllChannels
			}
			user.ChannelPatterns = append(user.ChannelPatterns, pattern)
		case strings.HasPrefix(rule, "+") || strings.HasPrefix(rule, "-"):
			// Command or category permission
			cmd := rule[1:]
			allowed := rule[0] == '+'
			if strings.HasPrefix(cmd, "@") {
				user.Categories[cmd] = allowed
			} else {
				user.Commands[cmd] = allowed
			}
		case rule == "resetchannels":
			user.ChannelPatterns = nil
			user.Flags &^= UserFlagAllChannels
		case rule == "resetkeys":
			user.KeyPatterns = nil
			user.Flags &^= UserFlagAllKeys
		case rule == "resetpass":
			user.Passwords = nil
		case rule == "reset":
			user.Passwords = nil
			user.Commands = make(map[string]bool)
			user.Categories = make(map[string]bool)
			user.KeyPatterns = nil
			user.ChannelPatterns = nil
			user.Flags = 0
		}
	}
	return nil
}

// DeleteUser deletes an ACL user.
func (a *ACL) DeleteUser(name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if name == "default" {
		return fmt.Errorf("ERR The 'default' user cannot be removed")
	}
	if _, ok := a.Users[name]; !ok {
		return fmt.Errorf("ERR user '%s' does not exist", name)
	}
	delete(a.Users, name)
	return nil
}

// GetUser returns an ACL user by name.
func (a *ACL) GetUser(name string) *ACLUser {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Users[name]
}

// ListUsers returns all users.
func (a *ACL) ListUsers() []*ACLUser {
	a.mu.RLock()
	defer a.mu.RUnlock()
	users := make([]*ACLUser, 0, len(a.Users))
	for _, u := range a.Users {
		users = append(users, u)
	}
	return users
}

// FormatUser returns the ACL SETUSER rules for a user.
func (a *ACL) FormatUser(user *ACLUser) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("user %s", user.Name))
	if user.Enabled {
		sb.WriteString(" on")
	} else {
		sb.WriteString(" off")
	}
	if len(user.Passwords) == 0 {
		sb.WriteString(" nopass")
	}
	for _, p := range user.Passwords {
		sb.WriteString(fmt.Sprintf(" #%s", p))
	}
	if user.Flags&UserFlagAllKeys != 0 {
		sb.WriteString(" ~*")
	} else {
		for _, p := range user.KeyPatterns {
			sb.WriteString(fmt.Sprintf(" ~%s", p))
		}
	}
	if user.Flags&UserFlagAllChannels != 0 {
		sb.WriteString(" &*")
	}
	for cmd, allowed := range user.Commands {
		if allowed {
			sb.WriteString(fmt.Sprintf(" +%s", cmd))
		} else {
			sb.WriteString(fmt.Sprintf(" -%s", cmd))
		}
	}
	for cat, allowed := range user.Categories {
		if allowed {
			sb.WriteString(fmt.Sprintf(" +%s", cat))
		} else {
			sb.WriteString(fmt.Sprintf(" -%s", cat))
		}
	}
	return sb.String()
}

func hashPassword(password string) string {
	h := sha256.Sum256([]byte(password))
	return fmt.Sprintf("%x", h)
}

func matchPattern(pattern, s string) bool {
	pi, si := 0, 0
	for pi < len(pattern) && si < len(s) {
		switch pattern[pi] {
		case '*':
			pi++
			if pi == len(pattern) {
				return true
			}
			for i := si; i <= len(s); i++ {
				if matchPattern(pattern[pi:], s[i:]) {
					return true
				}
			}
			return false
		case '?':
			pi++
			si++
		default:
			if s[si] != pattern[pi] {
				return false
			}
			pi++
			si++
		}
	}
	return pi == len(pattern) && si == len(s)
}
