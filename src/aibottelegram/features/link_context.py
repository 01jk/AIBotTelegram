import re
from urllib.parse import urlsplit

LinkInfo = dict[str, str | int]
GIFT_PATH = re.compile(r"/nft/([A-Za-z][A-Za-z0-9]{0,63})-([0-9]{1,10})/?", re.I | re.ASCII)
TELEGRAM_POST = re.compile(r"/(?:s/)?[A-Za-z][A-Za-z0-9_]{0,31}/[0-9]+/?", re.ASCII)


def describe_link(value: str) -> tuple[str, LinkInfo]:
    url = value.rstrip(".,!?;:…)]}»\"'")
    if "://" not in url:
        url = "https://" + url
    try:
        parsed = urlsplit(url)
        host = parsed.netloc.casefold().removeprefix("www.")
    except ValueError:
        return "внешняя ссылка", {"kind": "external_link"}

    if parsed.scheme.casefold() == "tg":
        return "ссылка telegram", {"kind": "telegram_link"}
    if host == "t.me":
        gift = GIFT_PATH.fullmatch(parsed.path)
        if gift:
            collection = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", gift[1])
            return "ссылка на подарок telegram", {
                "kind": "telegram_collectible_gift", "collection": collection, "item_number": int(gift[2]),
            }
        if TELEGRAM_POST.fullmatch(parsed.path):
            return "ссылка на пост telegram", {"kind": "telegram_post"}
        if parsed.path.startswith(("/+", "/joinchat/")):
            return "приглашение в чат telegram", {"kind": "telegram_invite"}
        return "ссылка telegram", {"kind": "telegram_link"}
    if host == "youtu.be" or (
        host in {"youtube.com", "m.youtube.com"}
        and (parsed.path == "/watch" or parsed.path.startswith(("/shorts/", "/live/")))
    ):
        return "видео youtube", {"kind": "youtube_video"}
    if host == "github.com":
        return "ссылка github", {"kind": "github_link"}
    if parsed.path.casefold().endswith(".pdf"):
        return "ссылка на pdf", {"kind": "pdf_link"}
    if parsed.path.casefold().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
        return "ссылка на изображение", {"kind": "image_link"}
    return "внешняя ссылка", {"kind": "external_link"}
