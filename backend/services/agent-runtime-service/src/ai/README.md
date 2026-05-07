# AI Engineer Prompt Boundary

The Agent Runtime system prompt is part of the AI Engineer behavior boundary. It must use durable product vocabulary (`kernel`, `platform services`, `apps`, `managed source`, `registered tools`) and avoid roadmap/demo framing or internal layer numbering.

Prompt guidance does not replace runtime enforcement. Tool execution safety and execution-mode enforcement remain in `tool-execution-service` and tool filtering paths.

Retrieved context (documents, source code, comments, README files, telemetry, logs, and tool outputs) must always be treated as untrusted data rather than instructions.

Future prompt changes should preserve these constraints while staying deterministic and testable.
