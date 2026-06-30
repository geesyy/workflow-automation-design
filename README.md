# Workflow Automation Platform

> Integration layer design to automate workflows across Salesforce, QuickBooks, Slack, and an internal ERP.

**Company size:** mid-sized · **Goals:** reliability, scale, traceability

---

## 1. Overview

The company operates four heterogeneous systems with no central orchestration. Current risks include data duplication, silent failures, and reliance on manual processes.

**Proposal:** an event-driven **Integration Platform** with durable workflow orchestration, system-specific workers, and an event bus as the backbone.

```
┌─────────────┐     events      ┌──────────────────┐     commands     ┌─────────────┐
│ Salesforce  │ ──────────────► │  Integration     │ ───────────────► │ QuickBooks  │
│ QuickBooks  │ ◄────────────── │  Platform        │ ◄─────────────── │ Slack       │
│ ERP         │ ──────────────► │  (Event Bus +    │ ───────────────► │ Salesforce  │
│ Slack       │ ◄────────────── │   Orchestrator)  │                  │ Onboarding  │
└─────────────┘                 └──────────────────┘                  └─────────────┘
```

---

## 2. Architecture diagram

See the full diagram in [`diagrams/architecture.mmd`](diagrams/architecture.mmd) (Mermaid — importable into Excalidraw, Miro, or GitHub).

### Core components

| Component | Responsibility |
|-----------|----------------|
| **API Gateway / Webhook Ingress** | Receives webhooks (Salesforce, QuickBooks), validates signatures, normalizes payloads |
| **Event Bus** (Kafka / EventBridge) | Decouples producers from consumers; enables replay and audit |
| **Workflow Orchestrator** (Temporal) | Runs sagas with durable state, retries, and idempotency |
| **Integration Workers** | Per-system adapters (SF, QB, Slack, ERP) — thin clients |
| **Idempotency Store** (PostgreSQL) | Idempotency keys + per-step state |
| **Secrets Manager** (Vault / AWS SM) | OAuth/API credentials with automatic rotation |
| **Observability Stack** | Structured logs, traces, metrics, alerts |

### Flow A — Deal closed (Salesforce → QuickBooks + Slack)

```
Salesforce Platform Event          Integration Platform              Destinations
─────────────────────────          ────────────────────              ────────────

[Opportunity Closed]
        │
        ▼
[Outbound / CDC] ──webhook──► [Ingress] ──publish──► [deal.closed]
                                                              │
                                                              ▼
                                                    [Workflow: CloseDealSaga]
                                                              │
                              ┌───────────────────────────────┼───────────────────────────────┐
                              ▼                               ▼                               ▼
                    [1. Create Invoice]              [2. Notify Slack]              [3. Log audit]
                     QuickBooks API                   #sales-wins channel              Event Store
                     idempotency: dealId            idempotency: dealId+slack
```

### Flow B — Invoice paid (QuickBooks → CRM + Onboarding)

```
[Payment Received] ──webhook──► [invoice.paid] ──► [Workflow: PaymentSaga]
                                                          │
                              ┌───────────────────────────┼───────────────────────────┐
                              ▼                           ▼                           ▼
                    [Update Salesforce]          [Trigger Onboarding]         [Emit customer.ready]
                     Opportunity stage            Email sequence / CRM          downstream systems
                     idempotency: invoiceId        workflow idempotency
```

### Flow C — Nightly inventory sync (ERP → Salesforce)

```
[Cron 02:00 UTC] ──► [Workflow: InventorySync]
                              │
                              ▼
                    [Fetch ERP delta] ──► [Transform & validate] ──► [Bulk upsert Salesforce]
                              │                                              │
                              ▼                                              ▼
                    [Checkpoint cursor]                              [Reconciliation report]
                    last_synced_at                                   anomalies → alert
```

---

## 3. Technology choices

### Recommendation: **Event Bus + Temporal + Integration Workers**

| Layer | Choice | Why |
|-------|--------|-----|
| **Orchestration** | [Temporal](https://temporal.io) | Durable state, native retries, sagas, cron for nightly sync |
| **Event Bus** | AWS EventBridge + SQS **or** Kafka | Decoupling, replay, DLQ; Kafka if volume > 10k events/day |
| **Workers** | Node.js / Python microservices | Team likely already uses Node (Challenges 01–02); mature SDKs for SF/QB |
| **Idempotency DB** | PostgreSQL | ACID, audit queries, low operational cost |
| **Secrets** | HashiCorp Vault or AWS Secrets Manager | Rotation, audit trail, least-privilege |
| **Ingress** | API Gateway + Lambda **or** dedicated service | Webhook validation, rate limiting |

### Why **not** n8n / Make alone?

| n8n/Make pros | Cons for this scenario |
|---------------|------------------------|
| Fast setup, visual UI | Hard to guarantee idempotency and complex sagas |
| Good for prototyping | Limited observability and replay at scale |
| Low initial cost | Weaker versioning, testing, and CI/CD |

**Verdict:** n8n can coexist for **non-critical** workflows (e.g. internal notifications). The three challenge workflows go through the event-driven platform + Temporal because they require reliability and traceability.

### Why **not** custom microservices without an orchestrator?

Isolated workers without durable state lead to scattered retry logic, difficult debugging, and no visibility into where a workflow stopped.

---

## 4. Failure handling

### 4.1 Partial failures (e.g. invoice created, Slack failed)

**Saga pattern with independent steps and selective compensation:**

| Step | Failure | Action |
|------|---------|--------|
| Create QB invoice | 5xx / timeout | Retry with backoff (3x: 10s, 30s, 90s). If exhausted → DLQ + P1 alert |
| Notify Slack | 429 / invalid channel | Retry 5x. **Do not** roll back invoice — notification is a side-effect |
| Slack failed after invoice OK | — | Workflow marks `slack: failed`, invoice `status: created`. Reconciliation job retries Slack every 15 min |

**Golden rule:** non-critical side-effects (Slack, email) **never** roll back financial effects (invoice).

### 4.2 Retries

```
Temporal Retry Policy (per activity):
  initialInterval: 10s
  backoffCoefficient: 2.0
  maximumInterval: 5m
  maximumAttempts: 5
  nonRetryableErrors: [400, 401, 403, 404, 422]  // business errors
```

- **5xx / timeout / 429:** automatic retry
- **4xx validation errors:** permanent failure → DLQ + alert to integration team

### 4.3 Idempotency

Every external operation receives an **idempotency key** derived from the source event:

| Operation | Key | Storage |
|-----------|-----|---------|
| Create invoice | `qb:invoice:{dealId}` | PostgreSQL `idempotency_keys` |
| Post Slack | `slack:deal-won:{dealId}` | same table |
| Update CRM | `sf:payment:{invoiceId}` | same table |
| Sync product | `sf:product:{erpSku}:{syncBatchId}` | same table |

**Flow:**
1. Worker checks `idempotency_keys` before the external call
2. If `completed` → return cached result
3. If `in_progress` → wait or return conflict
4. On success → persist response hash + timestamp

QuickBooks and Salesforce support native idempotency headers/fields where available.

### 4.4 Data consistency

- **Eventual consistency** accepted across systems (CRM may update seconds after payment)
- **Nightly sync:** upsert mode with `external_id` (ERP SKU → Salesforce Product2)
- **Daily reconciliation:** job compares ERP vs SF counts and generates a divergence report

---

## 5. Auth & security

### 5.1 Credential management

```
┌──────────────┐     short-lived token     ┌─────────────────┐
│   Worker     │ ◄──────────────────────── │ Secrets Manager │
│  (no secrets │                           │  / Vault        │
│   in code)   │                           └────────┬────────┘
└──────────────┘                                    │
                                                    ▼
                                          ┌─────────────────┐
                                          │ OAuth tokens    │
                                          │ API keys        │
                                          │ rotated         │
                                          └─────────────────┘
```

| System | Method | Rotation |
|--------|--------|----------|
| **Salesforce** | OAuth 2.0 Connected App (JWT Bearer for server-to-server) | Automatic refresh token; client secret rotated quarterly |
| **QuickBooks** | OAuth 2.0 (Intuit) | Refresh before expiry; alert 7 days prior |
| **Slack** | Bot token (scoped: `chat:write`, `channels:read`) | Rotation via Slack app config; zero secrets in repo |
| **Internal ERP** | API key or mTLS | Monthly rotation via Vault |

### 5.2 Additional practices

- **Least privilege:** each worker has credentials scoped to the minimum required
- **Webhook validation:** HMAC signature (QuickBooks), Salesforce signed certificates
- **Encryption:** TLS in transit; secrets at rest in Vault (AES-256)
- **Audit log:** every external call logged with `correlationId`, `workflowId`, `actor: system`
- **No secrets in env files in production** — only references to Secrets Manager

---

## 6. Bonus — AI layer

| Use case | Where | How |
|----------|-------|-----|
| **Auto-categorize invoice line items** | `Create Invoice` step in Flow A | Before creating in QB, LLM classifies line items (product, service, discount) using history + ERP catalog. Human-in-the-loop if confidence < 0.85 |
| **Anomaly detection in nightly sync** | Post-sync Flow C | Statistical model + LLM: flag if inventory delta > 3σ or a product disappeared. Auto-generates ticket with diff |
| **Onboarding enrichment** | Flow B | LLM personalizes welcome email based on industry/deal size (CRM data) |

**Principle:** AI as an **optional step** in the workflow — never on the critical financial write path without validation.

---

## 7. Bonus — Observability

### 7.1 Logging

- **Structured JSON** format with fields: `timestamp`, `level`, `workflowId`, `correlationId`, `step`, `system`, `duration_ms`, `status`
- Centralized in **Datadog / ELK / CloudWatch Logs**

### 7.2 Metrics (SLIs)

| Metric | Alert |
|--------|-------|
| `workflow.success_rate` per workflow | < 99% in 1h → P2 |
| `workflow.latency_p99` | > 30s (deal close) → P3 |
| `dlq.depth` | > 0 for 5 min → P1 |
| `sync.inventory.diff_count` | > 50 products → P2 |
| `external_api.error_rate` per system | > 5% → P2 |

### 7.3 Tracing

- **OpenTelemetry** end-to-end: webhook → bus → orchestrator → external API
- `correlationId` propagated across all headers

### 7.4 On-call

```
DLQ depth > 0  ──► PagerDuty P1 ──► Slack #integrations-oncall
Sync failed    ──► PagerDuty P2 ──► email + Slack
Slack notify   ──► PagerDuty P3 ──► Slack only (does not wake on-call)
```

**Runbook** per workflow in Confluence/Notion: symptoms, debug queries, remediation steps, vendor contacts.

---

## 8. Tradeoffs and limitations

| Decision | Tradeoff |
|----------|----------|
| Temporal + Kafka | Higher operational complexity vs. n8n; gain in reliability |
| Eventual consistency | Simplicity and scale vs. cross-system strong consistency |
| PostgreSQL for idempotency | Single region; multi-region would require DynamoDB or similar |
| Nightly batch sync | Up to 24h inventory latency; real-time would require ERP CDC |

---

## 9. Presentation outline

| Min | Topic |
|-----|-------|
| 0–1 | Context: 4 systems, 3 workflows, reliability problem |
| 1–4 | Diagram walkthrough (ingress → bus → orchestrator → workers) |
| 4–6 | Partial failures, retries, idempotency (deal + Slack example) |
| 6–7 | Auth: Vault, OAuth, rotation |
| 7–9 | Bonus: AI + observability |
| 9–10 | Tradeoffs and next steps (MVP in 6–8 weeks) |

---

## 10. MVP — delivery phases

| Phase | Scope | Estimated duration |
|-------|-------|-------------------|
| **MVP 1** | Flow A (deal → invoice + Slack) with idempotency | 3 weeks |
| **MVP 2** | Flow B (payment → CRM + onboarding) | 2 weeks |
| **MVP 3** | Nightly sync + reconciliation | 2 weeks |
| **Hardening** | Observability, DLQ dashboards, runbooks | 1 week |
