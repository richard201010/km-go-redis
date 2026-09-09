// 集群/哨兵/复制命令桩
package commands

import (
	"fmt"
	"strings"
)

// Cluster/Sentinel/Replication stubs
func clusterCommand(ctx *CommandContext) {
	if len(ctx.Args) > 1 {
		subcmd := strings.ToUpper(ctx.Args[1])
		switch subcmd {
		case "INFO":
			ctx.Client.SendBulkString("cluster_state:ok\r\ncluster_slots_assigned:0\r\ncluster_slots_ok:0\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:1\r\ncluster_size:1")
		case "MYID":
			ctx.Client.SendBulkString("0000000000000000000000000000000000000001")
		case "NODES":
			ctx.Client.SendBulkString("0000000000000000000000000000000000000001 127.0.0.1:6379@16379 myself,master - 0 0 connected")
		case "SLOTS":
			ctx.Client.SendArray(nil)
		case "RESET":
			ctx.Client.SendOK()
		default:
			ctx.Client.SendError(fmt.Sprintf("ERR Unknown CLUSTER subcommand '%s'", subcmd))
		}
	} else {
		ctx.Client.SendError("ERR wrong number of arguments for 'cluster' command")
	}
}
func askingCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}
func readonlyCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}
func readwriteCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}
func sentinelCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR unknown command 'sentinel'")
}
func failoverCommand(ctx *CommandContext) {
	ctx.Client.SendError("ERR FAILOVER is not supported in standalone mode")
}
func replicaofCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}
func replconfCommand(ctx *CommandContext) {
	ctx.Client.SendOK()
}
