from playwright.sync_api import sync_playwright
import sys
import asyncio
def get_youtube_transcript_text(video_url):
    full_transcript = ""
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )

        page = browser.new_page()
        page.goto(video_url)

        # 더보기 버튼 클릭
        try:
            page.click("tp-yt-paper-button#expand")
        except:
            print("⚠️ 더보기 버튼 클릭 실패(이미 열려있을 수도 있음)")

        try:
            page.click("button:has(span:text('스크립트 표시'))")
            print("✅ 스크립트 버튼 클릭 성공")
        except:
            print("⚠️ 스크립트 버튼 클릭 실패(스크립트 버튼이 없을 수 있음)")
            browser.close()
            return ""  # 버튼이 없으면 빈 문자열 반환

        # 자막 텍스트 추출
        page.wait_for_selector("yt-formatted-string.segment-text")
        segments = page.query_selector_all("yt-formatted-string.segment-text")
        transcript_texts = [seg.inner_text().strip() for seg in segments]
        full_transcript = "\n".join(transcript_texts)

        browser.close()

        return full_transcript

if __name__ == "__main__":
    video_url = "https://www.youtube.com/watch?v=AS1dpJe_epA"
    transcript = get_youtube_transcript_text(video_url)
    print("\n=== 최종 자막 ===")
    print(transcript)


