from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer

from .delivery import ManualDelivery
from .enrichment import enrich_lead
from .llm import LLMService
from .models import Lead
from .providers import get_provider
from .storage import load_leads, save_leads
from .utils import dedupe_leads

app = typer.Typer(help="lumen-scout: discover, enrich, and draft school outreach.")


@app.command()
def discover(
    city: str = typer.Option("Downey, CA", "--city"),
    max: int = typer.Option(25, "--max", min=1),
    provider: str = typer.Option("serpapi", "--provider"),
) -> None:
    """Discover private K-12 schools and append deduped leads to CSV."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    existing = load_leads()
    svc = get_provider(provider)
    found = svc.search(city=city, max_results=max)
    merged = dedupe_leads(existing, found)
    save_leads(merged)
    for lead in found:
        typer.echo(
            f"[DISCOVER] accepted lead='{lead.school_name}' query='{lead.source_query}' "
            f"website='{lead.website or 'n/a'}'"
            )
    typer.echo(f"Discovered {len(found)} results; saved {len(merged) - len(existing)} new leads to data/leads.csv")


@app.command()
def enrich(
    input: Path = typer.Option(Path("data/leads.csv"), "--input"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Enrich leads from website pages and generate personalization hook."""
    leads = load_leads(input)
    llm = LLMService(dry_run=dry_run)
    updated: list[Lead] = []
    for lead in leads:
        updated.append(enrich_lead(lead, llm))
    save_leads(updated, input)
    typer.echo(f"Enriched {len(updated)} leads -> {input}")


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

    leads = load_leads(input)
    llm = LLMService(dry_run=dry_run)
    delivery = ManualDelivery()

    count = 0
    for lead in leads:
        if count >= limit:
            break
        content = llm.email_draft(lead)
        output_path = Path("outreach_drafts") / f"{lead.lead_id}_email1.md"
        delivery.deliver(lead, content, output_path)
        lead.email1_path = str(output_path)
        count += 1

    save_leads(leads, input)
    typer.echo(f"Created {count} outreach drafts in ./outreach_drafts")
    typer.echo("Next steps: review markdown drafts, personalize as needed, and send manually.")


@app.command()
def followup(
    input: Path = typer.Option(Path("data/leads.csv"), "--input"),
    days: int = typer.Option(5, "--days", min=1),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Generate follow-up markdown drafts."""
    leads = load_leads(input)
    llm = LLMService(dry_run=dry_run)
    delivery = ManualDelivery()

    count = 0
    for lead in leads:
        content = llm.followup_draft(lead, days)
        output_path = Path("outreach_drafts") / f"{lead.lead_id}_followup_day{days}.md"
        delivery.deliver(lead, content, output_path)
        lead.followup_path = str(output_path)
        count += 1

    save_leads(leads, input)
    typer.echo(f"Created {count} follow-up drafts in ./outreach_drafts")
    typer.echo("Next steps: review follow-up markdown drafts and send manually.")


@app.command()
def brief(
    input: Path = typer.Option(Path("data/leads.csv"), "--input"),
    lead_id: str = typer.Option(..., "--lead-id"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Generate custom call brief markdown for one lead."""
    leads = load_leads(input)
    target: Optional[Lead] = next((lead for lead in leads if lead.lead_id == lead_id), None)
    if not target:
        raise typer.BadParameter(f"Lead id not found: {lead_id}")

    llm = LLMService(dry_run=dry_run)
    content = llm.call_brief(target)
    output_path = Path("call_briefs") / f"{target.lead_id}.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content.strip() + "\n", encoding="utf-8")

    target.brief_path = str(output_path)
    save_leads(leads, input)
    typer.echo(f"Created call brief: {output_path}")


if __name__ == "__main__":
    app()
