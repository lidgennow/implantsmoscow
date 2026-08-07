from pathlib import Path

from playwright.sync_api import sync_playwright


errors = []
output_dir = Path("/tmp/implantsmoscow-ui")
output_dir.mkdir(parents=True, exist_ok=True)

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 1000}, device_scale_factor=1)
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto("http://127.0.0.1:4173/docs/", wait_until="networkidle")
    page.locator("#kpi .kpi-card").first.wait_for()

    assert page.locator("#kpi .kpi-card").count() == 8
    assert page.get_by_text("ДРР", exact=True).count() == 0
    assert page.get_by_text("ПЦП на договоре", exact=True).count() == 0
    assert page.get_by_text("Implants Moscow", exact=False).count() == 0
    assert page.locator("#appts").count() == 0
    assert page.locator("#heroTotal").inner_text() != "—"
    assert page.locator("#heroZapis").inner_text() != "—"
    assert page.locator("#heroPrishel").inner_text() != "—"
    assert page.get_by_text("Рекламный расход", exact=True).count() == 1

    range_buttons = page.locator("#datePresets button")
    assert range_buttons.count() == 6
    page.locator("#dateFrom").fill("2026-04-01")
    page.locator("#dateTo").fill("2026-04-30")
    page.locator("#dateTo").press("Tab")
    page.wait_for_timeout(1100)
    assert "active" in (page.locator("#customRange").get_attribute("class") or "")
    assert "01.04.2026 — 30.04.2026" in page.locator("#period").inner_text()
    assert page.locator("#heroTotal").inner_text() == "242"
    assert page.locator("#heroZapis").inner_text() == "64"
    assert page.locator("#heroPrishel").inner_text() == "14"
    assert "294 100" in page.locator("#kpi .kpi-card").last.inner_text().replace(" ", " ")
    assert "Заявки — по созданию" in page.locator("#rangeHint").inner_text()

    range_buttons.filter(has_text="7 дней").click()
    assert "active" in (range_buttons.filter(has_text="7 дней").get_attribute("class") or "")
    page.wait_for_timeout(1100)

    page.get_by_role("tab", name="Прозвон").click()
    assert page.locator("#callKpi .kpi-card").count() == 6
    assert page.locator("#callOps tbody tr").count() >= 1
    assert page.get_by_text("подтверждённый разговор", exact=False).count() == 1
    page.screenshot(path=str(output_dir / "desktop-calls.png"), full_page=True)

    page.get_by_role("tab", name="Динамика").click()
    page.wait_for_timeout(300)
    assert page.locator("#chTrendLeads").count() == 1
    assert page.locator("#chTrendCalls").count() == 1
    page.screenshot(path=str(output_dir / "desktop-trend.png"), full_page=True)

    page.get_by_role("tab", name="Обзор").click()

    page.locator("#themeToggle").click()
    assert "dark" in (page.locator("body").get_attribute("class") or "")
    page.wait_for_timeout(1100)
    page.screenshot(path=str(output_dir / "desktop-dark.png"), full_page=False)
    page.locator("#themeToggle").click()
    assert "dark" not in (page.locator("body").get_attribute("class") or "")
    page.wait_for_timeout(1100)

    page.screenshot(path=str(output_dir / "desktop.png"), full_page=True)

    page.set_viewport_size({"width": 390, "height": 844})
    page.reload(wait_until="networkidle")
    page.locator("#kpi .kpi-card").first.wait_for()
    page.wait_for_timeout(1100)
    overflow = page.locator("body").evaluate(
        """() => [...document.querySelectorAll('*')]
            .filter(el => el.getBoundingClientRect().right > window.innerWidth + 1)
            .map(el => ({tag: el.tagName, cls: el.className, right: Math.round(el.getBoundingClientRect().right)}))
            .slice(0, 12)"""
    )
    body_fits = page.locator("body").evaluate("el => el.scrollWidth <= window.innerWidth")
    assert body_fits, f"Horizontal overflow: {overflow}"
    page.screenshot(path=str(output_dir / "mobile.png"), full_page=True)

    browser.close()

assert not errors, "Browser errors: " + " | ".join(errors)
print(f"UI smoke test passed. Screenshots: {output_dir}")
