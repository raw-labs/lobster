import type { CommandPack } from "../../commands/types.js";
import { emailTriageCommand } from "./email_triage.js";
import { gogGmailSearchCommand } from "./gmail_search.js";
import { gogGmailSendCommand } from "./gmail_send.js";

/** Gmail through the `gog` CLI, plus a triage command that sorts and drafts replies. */
export const googlePack: CommandPack = {
	name: "google",
	description: "Search and send Gmail through the gog CLI, and triage email.",
	commands: [gogGmailSearchCommand, gogGmailSendCommand, emailTriageCommand],
};
