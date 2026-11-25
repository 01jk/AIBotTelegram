import re
from collections import Counter

WORDS = re.compile(r"[а-яіїєґё]+", re.I)
PUNCTUATION = re.compile(r"[а-яіїєґё\s.,!?()…🙂😂👍]+", re.I)
UK = set("також дякую чому працює гаразд зараз добре зрозумів якщо ще тоді навіщо потрібно саме кажеш думаюся подивлюся спробуємо звісно поки мабуть бачу треба можемо згоден".split())
RU = set("почему спасибо работает хорошо понял сейчас если ещё тогда зачем нужно именно говоришь посмотрю попробуем конечно пока наверное вижу надо можем согласен понятно давай".split())
SAFE = UK | RU | set((
    "а и у я ти ты ви вы ми мы не но але ну да так нет ні это це то что шо що как як в у з с на про "
    "ок окей ага точно звісно верно вірно ясно поняв поняла зрозуміла правильно просто очень дуже "
    "думаю думаючи може можливо может можно можна хочу хочешь хочеш знаю знаєш спросить спитати "
    "уточнить уточнити скажи скажіть гляну посмотри подивись дивись глянь давайте давай "
    "глянем почекай подожди минуту хвилинку попробуй спробуй можно ли це чи вообще взагалі "
    "тебе тобі тобою тебе вам вас тебя дякую спасибі привет привіт здрасте здорово "
    "неплохо непогано хз имхо лол ахаха хаха ха і ще реально серйозно серьезно "
    "питання вопрос есть є вариант варіант цікаво интересно смысл сенс "
    "получится вийде идет йде ніби вроде если бы якби було было буде будет подивлюсь "
    "посмотрим подивимось спробуєм спробую попробую получается виходить "
    "мне мені тебя тобі це саме чому почему дяка зрозуміло хоч сейчас"
).split())
SLANG = {"хз", "имхо", "лол", "шо", "ага", "ок", "окей"}
LAUGHTER = {"аха", "ахаха", "хаха", "ха", "лол"}
RECONCILIATION = re.compile(
    r"\b(?:не\s+(?:будем\s+)?(?:ссор(?:иться|ьтесь|ься|юсь|имся)|руг(?:аться|айтесь|айся)"
    r"|свар(?:итися|іться|ися))|давай(?:те|мо)?\s+(?:мириться|миритися|помиримся|помирімось))\b",
    re.I,
)


def language(text: str, words: list[str]) -> str:
    lower = text.casefold()
    uk = sum(word in UK for word in words) + sum(lower.count(char) for char in "іїєґ")
    ru = sum(word in RU for word in words) + sum(lower.count(char) for char in "ыэёъ")
    return "uk" if uk > ru else "ru" if ru > uk else "und"


def situation(text: str, words: list[str]) -> str:
    if RECONCILIATION.search(text):
        return "reconciliation"
    vocab = set(words)
    if "?" in text or vocab & {"чому", "почему", "зачем", "навіщо", "уточнити", "уточнить"}:
        return "question"
    if vocab & {"дякую", "спасибо", "спасибі"}:
        return "thanks"
    if vocab & {"привіт", "привет"}:
        return "greeting"
    if vocab & {"давай", "давайте", "спробуємо", "попробуем", "гляну", "посмотрю", "подивлюся"}:
        return "suggestion"
    if vocab & {"ок", "добре", "понял", "зрозумів", "ясно", "гаразд", "хорошо"}:
        return "acknowledgement"
    return "reply"


def classify(text: str) -> tuple[str, str, list[str]]:
    words = WORDS.findall(text.casefold())
    return language(text, words), situation(text, words), words


def safe_example(text: str, words: list[str]) -> bool:
    # Admit only generic conversational vocabulary. A redaction heuristic alone
    # cannot tell whether an unfamiliar name or fact is private.
    return (
        1 <= len(words) <= 16 and len(text) <= 120 and "\n" not in text
        and PUNCTUATION.fullmatch(text) is not None and all(word in SAFE for word in words)
    )


class Profile:
    def __init__(self):
        self.lengths: Counter[int] = Counter()
        self.features: Counter[str] = Counter()
        self.openers: Counter[str] = Counter()
        self.endings: Counter[str] = Counter()
        self.slang: Counter[str] = Counter()

    def observe(self, text: str, lang: str, kind: str, words: list[str]) -> None:
        self.lengths[min(4096, len(text))] += 1
        self.features[f"lang_{lang}"] += 1
        self.features[f"situation_{kind}"] += 1
        self.features["multiline"] += "\n" in text
        self.features["question_mark"] += "?" in text
        self.features["exclamation_mark"] += "!" in text
        self.features["comma"] += "," in text
        self.features["emoji"] += any(0x1F300 <= ord(char) <= 0x1FAFF for char in text)
        self.features["formal"] += any(word in {"вы", "ви", "вам"} for word in words)
        self.features["informal"] += any(word in {"ты", "ти", "тебе", "тобі"} for word in words)
        self.features["laughter"] += bool(LAUGHTER.intersection(words)) or "😂" in text
        self.slang.update(word for word in words if word in SLANG)
        if words and words[0] in SAFE:
            self.openers[words[0]] += 1
        if words and words[-1] in SAFE:
            self.endings[words[-1]] += 1

    def report(self) -> dict:
        count = sum(self.lengths.values())
        midpoint = (count + 1) // 2
        median = 0
        running = 0
        for length, frequency in sorted(self.lengths.items()):
            running += frequency
            if running >= midpoint:
                median = length
                break
        return {
            "message_count": count, "median_characters": median, "features": dict(self.features),
            "openers": dict(self.openers.most_common(8)), "endings": dict(self.endings.most_common(8)),
            "slang": dict(self.slang.most_common(8)),
        }
