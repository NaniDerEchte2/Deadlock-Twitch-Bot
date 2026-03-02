import unittest

from bot.core.chat_bots import (
    KNOWN_CHAT_BOTS,
    build_known_chat_bot_not_in_clause,
    is_known_chat_bot,
)


class ChatBotsTests(unittest.TestCase):
    def test_is_known_chat_bot_is_case_insensitive(self) -> None:
        self.assertTrue(is_known_chat_bot("NightBot"))
        self.assertTrue(is_known_chat_bot("STREAMELEMENTS"))
        self.assertTrue(is_known_chat_bot("BoTRiX"))
        self.assertFalse(is_known_chat_bot("real_viewer_123"))

    def test_sql_helper_builds_stable_placeholders_and_params(self) -> None:
        clause, params = build_known_chat_bot_not_in_clause(
            column_expr="sc.chatter_login",
            placeholder="?",
        )
        self.assertIn("LOWER(sc.chatter_login) NOT IN", clause)
        self.assertEqual(clause.count("?"), len(params))
        self.assertEqual(params, sorted(KNOWN_CHAT_BOTS))

    def test_sql_helper_handles_empty_bot_list(self) -> None:
        clause, params = build_known_chat_bot_not_in_clause(
            column_expr="sc.chatter_login",
            bots=[],
        )
        self.assertEqual(clause, "1=1")
        self.assertEqual(params, [])


if __name__ == "__main__":
    unittest.main()
