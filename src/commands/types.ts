/** JSON Schema for a command's flags, as `parseArgs` produces them (`_` holds positionals). */
export type CommandArgsSchema = {
	type: "object";
	properties: Record<
		string,
		{ type?: string | string[]; description?: string; [key: string]: unknown }
	>;
	required?: string[];
};

export type CommandMeta = {
	description?: string;
	argsSchema?: unknown;
	examples?: Array<{ args: Record<string, unknown>; description?: string }>;
	/** Descriptive labels of what the command touches, shown by `commands.list`. */
	sideEffects?: string[];
	/**
	 * `false` when running the command twice could repeat an external action (send a message,
	 * call a tool). Lobster then never repeats it on its own: a retry stops once the command
	 * has reported dispatch through `ctx.onNonRetryableSideEffect`, and crash recovery never
	 * re-runs a step that contains it, even with `recovery: repeat`.
	 */
	repeatable?: boolean;
	/** Other names the command answers to. `ctx.commandName` holds the name that was used. */
	aliases?: string[];
	/**
	 * The command may create an input/approval gate before it begins execution.
	 * Commands that omit this are treated conservatively when a resumed pipeline
	 * is cancelled: its original capability cannot be replayed after dispatch.
	 */
	resumeSafeBeforeInput?: boolean;
	/**
	 * The command remains side-effect-free after returning a resumed input until
	 * the next pipeline stage dispatches. This is intentionally opt-in so a
	 * command that acts on a resumed response consumes its capability first.
	 */
	resumeSafeAfterInput?: boolean;
};

export type LobsterCommand = {
	name: string;
	/** `invokedAs` is the name the user typed, which may be an alias. */
	help: (invokedAs?: string) => string;
	run: (params: any) => Promise<any>;
	meta?: CommandMeta;
};

/** A command that declares everything Lobster relies on. All built-in commands are this. */
export type DefinedCommand = LobsterCommand & {
	meta: CommandMeta & {
		description: string;
		argsSchema: CommandArgsSchema;
		sideEffects: string[];
		repeatable: boolean;
	};
};

/** A named workflow that `workflows.list` shows and `workflows.run` runs. */
export type NamedWorkflow = {
	name: string;
	description: string;
	argsSchema: CommandArgsSchema;
	examples?: Array<{ args: Record<string, unknown>; description?: string }>;
	sideEffects: string[];
	/** `workflows.run` is declared repeatable, so every named workflow has to be. */
	repeatable: true;
	run: (params: { args: Record<string, any>; ctx: any }) => Promise<unknown>;
};

/** A set of commands and named workflows that is registered as a unit. */
export type CommandPack = {
	name: string;
	description: string;
	commands: DefinedCommand[];
	workflows?: NamedWorkflow[];
};

export type CommandRegistry = {
	get(name: string): LobsterCommand | undefined;
	/** Every name a command can be invoked by, aliases included, sorted. */
	list(): string[];
	/** Named workflows contributed by the registered packs. */
	workflows?(): NamedWorkflow[];
};
