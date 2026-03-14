from __future__ import annotations

import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any, Callable


def _normalize_bool(value: Any, default: bool = False) -> bool:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


@dataclass(slots=True, frozen=True)
class AffiliateEmailSettings:
    host: str
    port: int
    username: str
    password: str
    from_email: str
    from_name: str
    starttls: bool
    use_ssl: bool
    timeout_seconds: float = 20.0


class AffiliateEmailSender:
    def __init__(self, settings: AffiliateEmailSettings) -> None:
        self.settings = settings

    @classmethod
    def from_secret_loader(
        cls,
        loader: Callable[..., str] | None,
    ) -> AffiliateEmailSender | None:
        if loader is None:
            return None
        host = str(
            loader("AFFILIATE_GUTSCHRIFT_SMTP_HOST", "SMTP_HOST")
            or ""
        ).strip()
        if not host:
            return None

        port_raw = str(
            loader("AFFILIATE_GUTSCHRIFT_SMTP_PORT", "SMTP_PORT")
            or ""
        ).strip()
        try:
            port = int(port_raw or "587")
        except ValueError:
            port = 587

        from_email = str(
            loader(
                "AFFILIATE_GUTSCHRIFT_SMTP_FROM",
                "AFFILIATE_GUTSCHRIFT_FROM_EMAIL",
                "SMTP_FROM",
            )
            or ""
        ).strip()
        if not from_email:
            return None

        settings = AffiliateEmailSettings(
            host=host,
            port=max(1, port),
            username=str(
                loader("AFFILIATE_GUTSCHRIFT_SMTP_USERNAME", "SMTP_USERNAME")
                or ""
            ).strip(),
            password=str(
                loader("AFFILIATE_GUTSCHRIFT_SMTP_PASSWORD", "SMTP_PASSWORD")
                or ""
            ).strip(),
            from_email=from_email,
            from_name=str(
                loader(
                    "AFFILIATE_GUTSCHRIFT_SMTP_FROM_NAME",
                    "AFFILIATE_GUTSCHRIFT_FROM_NAME",
                )
                or "Deadlock Partner Network"
            ).strip(),
            starttls=_normalize_bool(
                loader("AFFILIATE_GUTSCHRIFT_SMTP_STARTTLS", "SMTP_STARTTLS"),
                default=True,
            ),
            use_ssl=_normalize_bool(
                loader("AFFILIATE_GUTSCHRIFT_SMTP_SSL", "SMTP_SSL"),
                default=False,
            ),
        )
        return cls(settings)

    def send_gutschrift(
        self,
        *,
        recipient_email: str,
        recipient_name: str,
        gutschrift_number: str,
        period_label: str,
        gross_amount_label: str,
        pdf_bytes: bytes,
        filename: str,
    ) -> None:
        mail = EmailMessage()
        display_name = str(recipient_name or "").strip() or "Affiliate"
        mail["Subject"] = f"Gutschrift {gutschrift_number} fuer {period_label}"
        mail["From"] = (
            f"{self.settings.from_name} <{self.settings.from_email}>"
            if self.settings.from_name
            else self.settings.from_email
        )
        mail["To"] = str(recipient_email or "").strip()
        mail.set_content(
            "\n".join(
                [
                    f"Hallo {display_name},",
                    "",
                    f"anbei deine Gutschrift {gutschrift_number} fuer den Zeitraum {period_label}.",
                    f"Auszahlungsbetrag brutto: {gross_amount_label}",
                    "",
                    "Die PDF ist dieser E-Mail beigefuegt.",
                ]
            )
        )
        mail.add_attachment(
            bytes(pdf_bytes),
            maintype="application",
            subtype="pdf",
            filename=filename,
        )

        smtp_cls = smtplib.SMTP_SSL if self.settings.use_ssl else smtplib.SMTP
        with smtp_cls(
            self.settings.host,
            self.settings.port,
            timeout=self.settings.timeout_seconds,
        ) as client:
            client.ehlo()
            if self.settings.starttls and not self.settings.use_ssl:
                client.starttls()
                client.ehlo()
            if self.settings.username:
                client.login(self.settings.username, self.settings.password)
            client.send_message(mail)
