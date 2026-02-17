from __future__ import annotations

import os

from openai import OpenAI

from .models import Lead

from dotenv import load_dotenv
load_dotenv()

class LLMService:
    def __init__(self, dry_run: bool = False) -> None:
        self.dry_run = dry_run
        self.client = None if dry_run else OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    def _complete(self, prompt: str, fallback: str) -> str:
        if self.dry_run:
            return fallback
        response = self.client.responses.create(
            model=self.model,
            input=prompt,
            temperature=0.3,
        )
        return response.output_text.strip() or fallback

    def personalization_hook(self, lead: Lead, page_text: str) -> str:
        prompt = (
            "Write one sentence for a private K-12 school administrator as a personalization hook. "
            "Keep it factual and specific based on this content:\n"
            f"School: {lead.school_name}\n"
            f"City: {lead.city}\n"
            f"Content: {page_text[:2500]}"
        )
        fallback = f"I noticed {lead.school_name} highlights a strong mission for students and families in {lead.city}."
        return self._complete(prompt, fallback)

    def email_draft(self, lead: Lead) -> str:
        prompt = (
            "Write an outreach email in markdown for a school administrator. "
            "Constraints: 60-90 words, no acronyms, do not use the words 'overlay' or 'AI', "
            "no pricing, no timeline promises, one call to action for a 15-minute call, "
            "professional school-administrator language.\n"
            f"School: {lead.school_name}\n"
            f"Personalization hook: {lead.personalization_hook or ''}"
        )
        fallback = (
            f"Hi {lead.school_name} team,\n\n"
            f"I noticed your school emphasizes student support and family partnership. "
            "We help school leaders reduce routine staff workload and improve follow-through in daily operations. "
            "If helpful, I can share a simple example tailored to your context. "
            "Would you be open to a 15-minute call next week?\n"
        )
        return self._complete(prompt, fallback)

    def followup_draft(self, lead: Lead, days: int) -> str:
        prompt = (
            "Write a polite follow-up email in markdown for a school administrator. "
            "Constraints: 60-90 words, no acronyms, do not use the words 'overlay' or 'AI', "
            "no pricing, no timeline promises, one call to action for a 15-minute call.\n"
            f"School: {lead.school_name}\nDays since initial outreach: {days}\n"
            f"Personalization hook: {lead.personalization_hook or ''}"
        )
        fallback = (
            f"Hi {lead.school_name} team,\n\n"
            "I wanted to briefly follow up in case my earlier note was buried. "
            "We support school administrators with practical workflow improvements that help staff stay focused on students and families. "
            "If it is useful, I can share one relevant example for your campus. "
            "Would you be open to a 15-minute call?\n"
        )
        return self._complete(prompt, fallback)

    def linkedin_draft(self, lead: Lead) -> str:
        prompt = (
            "Write a concise LinkedIn outreach message for a private K-12 school decision maker. "
            "Constraints: 45-70 words, professional tone, one call to action for a 15-minute call.\n"
            f"School: {lead.school_name}\n"
            f"Personalization hook: {lead.personalization_hook or ''}"
        )
        fallback = (
            f"Hi, I work with private schools like {lead.school_name} to reduce routine administrative load "
            "and improve follow-through for staff and families. "
            "If helpful, I can share one practical example relevant to your school. "
            "Would you be open to a brief 15-minute conversation?"
        )
        return self._complete(prompt, fallback)

    def contact_form_draft(self, lead: Lead) -> str:
        prompt = (
            "Write a contact-form-safe outreach message for a private K-12 school. "
            "Constraints: plain text, 50-80 words, no markdown, one CTA for a 15-minute call.\n"
            f"School: {lead.school_name}\n"
            f"Personalization hook: {lead.personalization_hook or ''}"
        )
        fallback = (
            f"Hello {lead.school_name} team, I am reaching out because we help school leaders reduce routine "
            "administrative workload and improve day-to-day follow-through. "
            "If useful, I can share one simple example tailored to your school context. "
            "Would a 15-minute call next week be possible?"
        )
        return self._complete(prompt, fallback)

    def call_brief(self, lead: Lead) -> str:
        prompt = (
            "Create a concise call brief in markdown for preparing a first conversation with a private K-12 school administrator. "
            "Include: context summary, likely priorities, discovery questions, objection handling, and next-step ask.\n"
            f"School: {lead.school_name}\nCity: {lead.city}\n"
            f"Hook: {lead.personalization_hook or ''}"
        )
        fallback = (
            f"# Call Brief: {lead.school_name}\n\n"
            "## Context Summary\n"
            f"- Private K-12 school in {lead.city}.\n"
            "- Emphasis on operational consistency and family communication.\n\n"
            "## Likely Priorities\n- Staff workload balance\n- Student support consistency\n- Family responsiveness\n\n"
            "## Discovery Questions\n- Where does administrative follow-through break down most often?\n"
            "- Which routines consume staff time each week?\n"
            "- What outcomes matter most this term?\n\n"
            "## Objection Handling\n- Keep approach practical and lightweight.\n"
            "- Focus on existing workflows and staff capacity.\n\n"
            "## Next-Step Ask\n- Confirm a 15-minute follow-up with key stakeholders.\n"
        )
        return self._complete(prompt, fallback)
