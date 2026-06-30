# Workflow Automation Platform — One-Pager

> Quick visual reference for the platform design

---

## Architecture in one sentence

**Events enter via webhooks → Event Bus decouples → Temporal orchestrates sagas with retries → Workers call external APIs with idempotency.**

---

## Three workflows

| # | Trigger | Actions | Idempotency key |
|---|---------|---------|-----------------|
| **A** | Deal closed (SF) | Invoice (QB) + Slack | `dealId` |
| **B** | Invoice paid (QB) | Update CRM + Onboarding | `invoiceId` |
| **C** | Nightly cron | ERP → SF inventory sync | `sku + batchId` |

---

## Recommended stack

```
Temporal  +  Event Bus (Kafka/EventBridge)  +  PostgreSQL (idempotency)
         +  Vault (secrets)  +  OpenTelemetry (observability)
```

---

## Golden rule for failures

> **Financial effects are not rolled back due to notification failures.**

Invoice created + Slack failed = retry Slack, do not delete the invoice.

---

## Diagram

Import `diagrams/architecture.mmd` into:
- [Mermaid Live Editor](https://mermaid.live)
- Excalidraw (Mermaid plugin)
- GitHub (auto-render)

---

## For non-technical stakeholders

1. **Less manual work** — deals and payments flow automatically
2. **Fewer errors** — the system remembers what it already did (no duplicate invoices)
3. **Visibility** — if something breaks, the team is alerted within minutes
4. **Security** — system passwords stored in a vault, not in spreadsheets
