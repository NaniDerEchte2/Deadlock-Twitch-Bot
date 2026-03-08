export interface Command {
  input: string;
  output: string;
  delay?: number;
}

export const commands: Command[] = [
  {
    input: "!raid",
    output: "[EarlySalty] Raid gestartet! Ziel: StreamerXYZ (42 Viewer)",
    delay: 0,
  },
  {
    input: "!clip Krasser Play",
    output: '[EarlySalty] Clip erstellt: "Krasser Play" (32s)',
    delay: 1200,
  },
  {
    input: "!stats",
    output: "[EarlySalty] Heute: 3.2h gestreamt | Ø 128 Viewer | Peak: 247",
    delay: 2400,
  },
  {
    input: "!ping",
    output: "[EarlySalty] Bot ist online! Latenz: 23ms",
    delay: 3600,
  },
];
