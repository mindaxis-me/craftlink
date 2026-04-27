package me.mindaxis.view;

import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.Player;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handles the /viewer command:
 *   /viewer start   — start WS server and begin streaming
 *   /viewer stop    — stop WS server and cleanup
 *   /viewer anchor <player> — set camera anchor player
 *   /viewer status  — show current status
 */
public class ViewerCommand implements CommandExecutor, TabCompleter {

    private final MindAxisViewPlugin plugin;

    public ViewerCommand(MindAxisViewPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (args.length == 0) {
            sender.sendMessage("§6[MindAxisView] §fUsage: /viewer <start|stop|anchor|status>");
            return true;
        }

        switch (args[0].toLowerCase()) {
            case "start" -> {
                if (plugin.isRunning()) {
                    sender.sendMessage("§6[MindAxisView] §cAlready running!");
                    return true;
                }
                plugin.startViewer();
                sender.sendMessage("§6[MindAxisView] §aStarted! WS on port " + plugin.getWsPort());
            }
            case "stop" -> {
                if (!plugin.isRunning()) {
                    sender.sendMessage("§6[MindAxisView] §cNot running!");
                    return true;
                }
                plugin.stopViewer();
                sender.sendMessage("§6[MindAxisView] §eStopped.");
            }
            case "anchor" -> {
                if (args.length < 2) {
                    sender.sendMessage("§6[MindAxisView] §fUsage: /viewer anchor <player>");
                    return true;
                }
                Player target = Bukkit.getPlayer(args[1]);
                if (target == null) {
                    sender.sendMessage("§6[MindAxisView] §cPlayer not found: " + args[1]);
                    return true;
                }
                plugin.setAnchorPlayer(target);
                sender.sendMessage("§6[MindAxisView] §aAnchor set to " + target.getName());
            }
            case "status" -> {
                if (!plugin.isRunning()) {
                    sender.sendMessage("§6[MindAxisView] §7Not running.");
                } else {
                    Player anchor = plugin.getAnchorPlayer();
                    String anchorName = anchor != null ? anchor.getName() : "(none)";
                    int clients = plugin.getWsServer() != null ? plugin.getWsServer().getClientCount() : 0;
                    sender.sendMessage("§6[MindAxisView] §aRunning §7| Anchor: §f" + anchorName
                            + " §7| Clients: §f" + clients
                            + " §7| Port: §f" + plugin.getWsPort());
                }
            }
            default -> sender.sendMessage("§6[MindAxisView] §fUsage: /viewer <start|stop|anchor|status>");
        }
        return true;
    }

    @Override
    public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
        if (args.length == 1) {
            return filterStartsWith(Arrays.asList("start", "stop", "anchor", "status"), args[0]);
        }
        if (args.length == 2 && "anchor".equalsIgnoreCase(args[0])) {
            return filterStartsWith(
                    Bukkit.getOnlinePlayers().stream().map(Player::getName).collect(Collectors.toList()),
                    args[1]);
        }
        return List.of();
    }

    private List<String> filterStartsWith(List<String> options, String prefix) {
        String lower = prefix.toLowerCase();
        return options.stream()
                .filter(s -> s.toLowerCase().startsWith(lower))
                .collect(Collectors.toList());
    }
}
