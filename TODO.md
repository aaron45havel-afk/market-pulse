# FocusedOps — Roadmap

Persistent todo list. Newest entries on top of each section.

## 🔥 Tier 2 transition — when a client wants real data

Trigger: client says any of these:
- "Let me hook up our real Sage 300 credentials"
- "I want my colleagues to use this daily"
- "Can we feed our real vendor / customer data through it?"
- "We'll just keep using your URL"

### Build queue

- [ ] **Add auth to Vercel-hosted prototypes** (~30 min per prototype, mostly reusable)
  - Pattern: drop-in Google OAuth via the same flow we built into market-pulse (see `market-pulse-v21/auth.py`)
  - Could ship as a small starter template repo with Google sign-in pre-wired
  - Should expose env vars: `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `ALLOWED_EMAILS`
- [ ] **Move client to VERCEL PROJECT stage with hosting=Managed**
  - Tick "Auth method picked" in Pilot Agreement E
  - Set Hosting field to "Managed"
- [ ] **Sign the Data Processing Addendum (DPA) before any real data flows**
  - Template: LawDepot or RocketLawyer (~$40)
  - Must cover: data residency, retention, breach notification, sub-processors (Vercel, Railway, Resend, Anthropic)
  - Reference: Pilot Agreement section F

### CRM features to ship for this transition

- [ ] **Tier 2 transition button** on contact card — flips status + opens the auth-setup playbook
- [ ] **DPA upload field** on the contact (PDF storage — could use Railway Volumes or Vercel Blob)
- [ ] **Starter-template repo on GitHub** that the 🚀 button can clone instead of `auto_init`
  - Pre-wired with: Google OAuth, `/api/me` route, `<AuthGate>` wrapper
  - When client wants auth, redeploy from this template
- [ ] **Auth status badge** on the Testing card (🔓 open / 🔒 auth on)

---

## 💼 Pilot agreement maturation

- [ ] **PDF export** of the Pilot Agreement Checklist — turn it into a fill-in contract
  - One-click "Export as PDF" from the 🤝 modal
  - Use a template engine (WeasyPrint or just HTML→PDF)
- [ ] **Generic FocusedOps Master Services Agreement (MSA)** — boilerplate above per-pilot SOWs
  - Have a startup attorney review (~$500-1000 one-time)
- [ ] **Cyber + CGL insurance** quotes via Hiscox / Thimble
- [ ] **Pricing anchor table** in the docs — sub-$50M client = $1k-1.5k/mo; mid = $1.5k-3k; large = $3k-7k

---

## 🚀 Vercel automation improvements

- [ ] **GitHub repo template** field on the 🚀 modal — clone from a starter instead of empty repo
- [ ] **Auto-trigger first deploy** after Vercel project creation (so the URL stops 404'ing without a push)
- [ ] **Vercel team support** — let user pick which team owns the new project (currently hardcoded to personal account)

---

## 🌐 focusedops.io (the marketing site)

- [ ] **Case study page** once Jim's pilot wraps (with his approval)
  - Even a single one-pager is the strongest thing on a consulting site
- [ ] **Insight posts** (~1-2 per month) — short writeups on specific problems we solve
  - SEO + credibility — 400 words is enough
- [ ] **Logo / wordmark** — simple sans-serif in Figma, ship a favicon update
- [ ] **Squarespace trial** — cancel before auto-renewal (Vercel-hosted now, no need for Squarespace plan)

---

## 🏛 Business setup (CA)

- [ ] **CA LLC formation** — bizfileonline.sos.ca.gov, $70 + $20 within 90 days
- [ ] **Operating agreement** with Jim — partner split, vesting, IP, hosting model, exit terms
  - DRAFT: review the partnership questions from our earlier conversation:
    - Ownership %
    - Vesting (recommend 4-yr / 1-yr cliff)
    - Roles and decision authority
    - Buyout terms
    - IP assignment to LLC
- [ ] **EIN** from irs.gov (free, instant)
- [ ] **Business checking** — Mercury / Relay
- [ ] **$800 CA franchise tax** — first payment due 15th of 4th month after formation

---

## 📈 CRM enhancements (nice-to-have)

- [ ] **Calendar integration** — pull next_date deadlines into Google Calendar
- [ ] **Bulk import** — CSV upload of cold-prospect lists
- [ ] **Email open tracking** via Resend webhooks
- [ ] **Funnel cohort analytics** — month-over-month conversion by industry × role
- [ ] **Pipeline view filter** — filter by industry / role / pilot $ to focus

---

*Last edited: 2026-06-14 · session 013kvyp3pJR5uVW9A473maWv*
