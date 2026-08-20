# Video Dataset Curation Agent: Source → Verify → Deliver

Ability to build the exact three-stage pipeline Velvet describes (source, verify, deliver) using agentic LLM tooling for judgment calls a human curator would otherwise make.

**Live demo:** https://velvet.ashanpraba.com

The demo runs entirely in the browser against seeded data — no API keys,
no accounts, and no external services required.

## Stack

- Python
- LangChain
- Bedrock
- Redis
- AWS S3
- Go (CLI wrapper)

## How it works

- A folder of 8-10 sample video metadata records (JSON: duration, scene description, tags, presence of faces/text/watermarks).
- Write a LangChain agent (Bedrock-backed) with two tool calls: 'compliance_check' (flags PII/faces/watermarks) and 'spatial_quality_score' (rates camera motion, multi-object interaction, depth cues 1-10).
- Run each record through the agent, cache results in Redis keyed by clip ID with TTL to simulate a verification queue.
- Aggregate passing, high-quality records into a structured delivery manifest (JSON) and push it to an S3 bucket.
- Write a small Go CLI that queries Redis + S3 to show pipeline status (queued/verified/delivered) for the demo narration.
- Record a 60-90s walkthrough: ingest → agent verifies → manifest delivered, narrating each tool call live.

## Running locally

```bash
cd src
bash run.sh
```

Then open the printed URL. A prebuilt static version of the UI lives in
`src/web/` and can be opened directly with no server.
