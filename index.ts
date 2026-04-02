import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { emptyPluginConfigSchema } from "openclaw/plugin-sdk";
import { streamchatPlugin } from "./src/channel.js";
import { setStreamChatRuntime } from "./src/runtime.js";

const plugin = {
  id: "openclaw-channel-streamchat",
  name: "Stream Chat",
  description: "Stream Chat messaging channel for OpenClaw",
  configSchema: emptyPluginConfigSchema(),
  register(api: OpenClawPluginApi): void {
    setStreamChatRuntime(api.runtime);
    api.registerChannel({ plugin: streamchatPlugin });
  },
};

export default plugin;
