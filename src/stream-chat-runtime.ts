import { StreamChat } from "stream-chat";
import type { Channel, Event } from "stream-chat";
import type { ChannelLogSink } from "openclaw/plugin-sdk";
import type { ResolvedAccount } from "./types.js";

export class StreamChatClientRuntime {
  private client: StreamChat;
  private channels = new Map<string, Channel>();
  private account: ResolvedAccount;
  private log?: ChannelLogSink;
  private connected = false;
  private addedToChannelHandler?: (event: Event) => void;

  constructor(account: ResolvedAccount, log?: ChannelLogSink) {
    this.account = account;
    this.log = log;
    this.client = new StreamChat(account.apiKey, {
      allowServerSideConnect: true,
    });
  }

  async start(): Promise<void> {
    const { botUserId, botUserToken, botUserName } = this.account;

    this.log?.info?.(`[StreamChat] Connecting as ${botUserId}...`);

    await this.client.connectUser(
      { id: botUserId, name: botUserName || botUserId },
      botUserToken,
    );
    this.connected = true;

    this.log?.info?.(`[StreamChat] Connected. Querying channels...`);

    const filters = { members: { $in: [botUserId] } };
    const sort = [{ last_message_at: -1 as const }];
    const channelList = await this.client.queryChannels(filters, sort, {
      watch: true,
      limit: 30,
    });

    for (const ch of channelList) {
      const key = `${ch.type}:${ch.id}`;
      this.channels.set(key, ch);
    }

    this.log?.info?.(`[StreamChat] Watching ${channelList.length} channel(s).`);

    // Auto-watch new channels the bot is added to
    this.addedToChannelHandler = (event: Event) => {
      if (event.channel) {
        const ch = this.client.channel(event.channel.type, event.channel.id);
        ch.watch()
          .then(() => {
            const key = `${event.channel!.type}:${event.channel!.id}`;
            this.channels.set(key, ch);
            this.log?.info?.(`[StreamChat] Auto-watching new channel ${key}`);
          })
          .catch((err) => {
            this.log?.error?.(
              `[StreamChat] Failed to watch channel: ${String(err)}`,
            );
          });
      }
    };
    this.client.on("notification.added_to_channel", this.addedToChannelHandler);
  }

  /**
   * Full disconnect + fresh client + reconnect cycle.
   * Used by the ConnectionWatchdog when the SDK's own reconnection silently fails.
   *
   * The method is designed to be "atomic" with respect to `this.client`:
   * the new SDK instance is only assigned after it has successfully connected.
   * If `start()` fails, `this.client` still points at the old (disconnected)
   * instance so that callers holding the reference see a consistent state,
   * and the error propagates to the watchdog for retry.
   */
  async reconnect(): Promise<void> {
    this.log?.info?.("[StreamChat] Reconnecting...");

    const oldClient = this.client;

    // Tear down existing connection
    try {
      if (this.addedToChannelHandler) {
        oldClient.off(
          "notification.added_to_channel",
          this.addedToChannelHandler,
        );
        this.addedToChannelHandler = undefined;
      }
      await oldClient.disconnectUser();
    } catch (err) {
      this.log?.warn?.(
        `[StreamChat] Disconnect during reconnect failed: ${String(err)}`,
      );
    }
    this.connected = false;
    this.channels.clear();

    // Create a fresh client to avoid reusing a potentially broken SDK state.
    // Assign to this.client *before* start() because start() reads this.client.
    const newClient = new StreamChat(this.account.apiKey, {
      allowServerSideConnect: true,
    });
    this.client = newClient;

    try {
      // Reconnect with the same credentials
      await this.start();
    } catch (err) {
      // Restore the old client reference so getClient() returns a
      // consistent (albeit disconnected) instance rather than a
      // half-initialized one.  The watchdog will retry shortly.
      this.client = oldClient;
      throw err;
    }
  }

  async stop(): Promise<void> {
    if (this.connected) {
      if (this.addedToChannelHandler) {
        this.client.off(
          "notification.added_to_channel",
          this.addedToChannelHandler,
        );
        this.addedToChannelHandler = undefined;
      }
      this.log?.info?.(`[StreamChat] Disconnecting...`);
      await this.client.disconnectUser();
      this.connected = false;
      this.channels.clear();
      this.log?.info?.(`[StreamChat] Disconnected.`);
    }
  }

  getClient(): StreamChat {
    return this.client;
  }

  getChannel(type: string, id: string): Channel | undefined {
    return this.channels.get(`${type}:${id}`);
  }

  async getOrQueryChannel(type: string, id: string): Promise<Channel> {
    const existing = this.channels.get(`${type}:${id}`);
    if (existing) return existing;

    const ch = this.client.channel(type, id);
    await ch.watch();
    this.channels.set(`${type}:${id}`, ch);
    return ch;
  }

  isConnected(): boolean {
    return this.connected;
  }
}
