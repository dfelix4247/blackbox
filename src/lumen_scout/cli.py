from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer

from .core_store import (
    ensure_seeded_from_csv,
    export_legacy_schools_csv,
    get_all_leads,
    upsert_school_lead,
)
from .delivery import ManualDelivery
from .enrichment import enrich_lead
from .llm import LLMService
from .models import Lead
from .providers import get_provider


app = typer.Typer(help="lumen-scout: discover, enrich, and draft school outreach.")


@app.command()
def discover(
    city: str = typer.Option("Downey, CA", "--city"),
    max: int = typer.Option(25, "--max", min=1),
    provider: str = typer.Option("serpapi", "--provider"),
) -> None:
    """Discover private K-12 schools and upsert leads into scout-core storage."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    before_count = len(get_all_leads())
    svc = get_provider(provider)
    found = svc.search(city=city, max_results=max)
    
    for lead in found:
        typer.echo(
            f"[DISCOVER] accepted lead='{lead.school_name}' query='{lead.source_query}' "
            f"website='{lead.website or 'n/a'}'"
        )

    csv_path = export_legacy_schools_csv()
    after_count = len(get_all_leads())
    typer.echo(f"Discovered {len(found)} results; saved {max(after_count - before_count, 0)} new leads to {csv_path}")    


@app.command()
def enrich(
    input: Path = typer.Option(Path("data/leads.csv"), "--input"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Enrich leads from website pages and generate personalization hook."""
    ensure_seeded_from_csv(input)
    leads = get_all_leads()

    llm = LLMService(dry_run=dry_run)
    updated: list[Lead] = []
    for lead in leads:
        enriched = enrich_lead(lead, llm)
        upsert_school_lead(enriched)
        updated.append(enriched)

    csv_path = export_legacy_schools_csv(input)
    typer.echo(f"Enriched {len(updated)} leads -> {csv_path}")


@app.command()
def draft(
    input: Path = typer.Option(Path("data/leads.csv"), "--input"),
    limit: int = typer.Option(10, "--limit", min=1),
    delivery_mode: str = typer.Option("manual", "--delivery-mode"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Generate initial outreach draft markdown files."""
    if delivery_mode != "manual":
        raise typer.BadParameter("Only manual delivery-mode is supported in v1")

    ensure_seeded_from_csv(input)
    leads = get_all_leads()
    llm = LLMService(dry_run=dry_run)
    delivery = ManualDelivery()

    sorted_leads = sorted(leads, key=lambda lead: lead.contact_score, reverse=True)
    count = 0
    for lead in sorted_leads:
        if count >= limit:
            break
        method = lead.contact_method or "none"
        tier = lead.contact_priority_label or "Tier 5"
        if tier in {"Tier 1", "Tier 3"}:
            content = llm.email_draft(lead)
            output_path = Path("outreach_drafts") / f"{lead.lead_id}_email1.md"
            delivery.deliver(lead, content, output_path)
            lead.email1_path = str(output_path)
        elif tier == "Tier 4":
            content = llm.contact_form_draft(lead)
            output_path = Path("outreach_drafts") / f"{lead.lead_id}_contact_form.md"
            delivery.deliver(lead, content, output_path)
            lead.email1_path = str(output_path)
        elif tier == "Tier 5" and method == "phone_only":
            continue

        if tier in {"Tier 1", "Tier 2"} and lead.linkedin_url:
            linkedin_content = llm.linkedin_draft(lead)
            linkedin_output_path = Path("outreach_drafts") / f"{lead.lead_id}_linkedin.md"
            delivery.deliver(lead, linkedin_content, linkedin_output_path)

        if tier in {"Tier 1", "Tier 2"} and lead.contact_email and not lead.email1_path:
            email_content = llm.email_draft(lead)
            email_output_path = Path("outreach_drafts") / f"{lead.lead_id}_email1.md"
            delivery.deliver(lead, email_content, email_output_path)
            lead.email1_path = str(email_output_path)

        upsert_school_lead(lead)
        count += 1

    export_legacy_schools_csv(input)
    typer.echo(f"Created {count} outreach drafts in ./outreach_drafts")
    typer.echo("Next steps: review markdown drafts, personalize as needed, and send manually.")


@app.command()
def followup(
    input: Path = typer.Option(Path("data/leads.csv"), "--input"),
    days: int = typer.Option(5, "--days", min=1),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Generate follow-up markdown drafts."""
    ensure_seeded_from_csv(input)
    leads = get_all_leads()
    llm = LLMService(dry_run=dry_run)
    delivery = ManualDelivery()

    count = 0
    for lead in leads:
        content = llm.followup_draft(lead, days)
        output_path = Path("outreach_drafts") / f"{lead.lead_id}_followup_day{days}.md"
        delivery.deliver(lead, content, output_path)
        lead.followup_path = str(output_path)
        upsert_school_lead(lead)
        count += 1

    export_legacy_schools_csv(input)
    typer.echo(f"Created {count} follow-up drafts in ./outreach_drafts")
    typer.echo("Next steps: review follow-up markdown drafts and send manually.")


@app.command()
def brief(
    input: Path = typer.Option(Path("data/leads.csv"), "--input"),
    lead_id: str = typer.Option(..., "--lead-id"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Generate custom call brief markdown for one lead."""
    ensure_seeded_from_csv(input)
    leads = get_all_leads()
    target: Optional[Lead] = next((lead for lead in leads if lead.lead_id == lead_id), None)
    if not target:
        raise typer.BadParameter(f"Lead id not found: {lead_id}")

    llm = LLMService(dry_run=dry_run)
    content = llm.call_brief(target)
    output_path = Path("call_briefs") / f"{target.lead_id}.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content.strip() + "\n", encoding="utf-8")

    target.brief_path = str(output_path)
    upsert_school_lead(target)
    export_legacy_schools_csv(input)
    typer.echo(f"Created call brief: {output_path}")


if __name__ == "__main__":
    app()
