from dataclasses import dataclass


@dataclass(frozen=True)
class ConversationScheduler:
    chat_id: int | None
    thread_id: int
    started_at: int
    last_at: int
    switch_after: int

    def active_until(self, limits: dict[str, int]) -> int:
        return min(
            self.started_at + limits["conversation_max"],
            self.last_at + limits["conversation_idle"],
        )

    def is_active(self, now: int, limits: dict[str, int]) -> bool:
        return self.chat_id is not None and now < self.active_until(limits)

    def permits(self, chat_id: int, thread_id: int, now: int, limits: dict[str, int]) -> bool:
        if not limits["conversation_max"] or not limits["conversation_idle"]:
            return False
        if self.is_active(now, limits):
            return (self.chat_id, self.thread_id) == (chat_id, thread_id)
        if self.chat_id is not None and now < self.active_until(limits) + limits["switch_cooldown"]:
            return False
        return now >= self.switch_after
