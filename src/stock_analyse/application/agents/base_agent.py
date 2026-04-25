from __future__ import annotations

import json
from typing import Any


DEFAULT_AGENT_SYSTEM_PROMPT = '你是严谨的中文股票分析师，请基于提供的数据做结构化分析，不要编造未给出的事实。'


class BaseStockAnalysisAgent:
    def __init__(
        self,
        *,
        role_name: str,
        instruction: str,
        ai_platform: str | None = None,
        ai_model: str | None = None,
        api_code: str | None = None,
        system_prompt: str | None = None,
    ) -> None:
        from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer

        self.role_name = role_name
        self.instruction = instruction
        self.analyzer = StockAiAnalyzer(
            system_prompt=system_prompt or DEFAULT_AGENT_SYSTEM_PROMPT,
            prompt_template='{content}',
            ai_platform=ai_platform,
            model=ai_model,
            api_token=api_code,
        )

    def build_prompt(self, context: dict[str, Any]) -> str:
        payload = json.dumps(context, ensure_ascii=False, indent=2, default=str)
        return (
            f'角色: {self.role_name}\n'
            f'任务说明:\n{self.instruction}\n\n'
            '请严格输出 JSON 对象，字段为: '\
            'summary, signals, risks, confidence, evidence。\n\n'
            f'上下文数据:\n{payload}'
        )

    def parse_response(self, response: str) -> dict[str, Any]:
        text = (response or '').strip()
        if not text:
            return self._fallback_response('')
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1 and end > start:
            candidate = text[start:end + 1]
            try:
                data = json.loads(candidate)
                return {
                    'summary': data.get('summary', ''),
                    'signals': data.get('signals', []),
                    'risks': data.get('risks', []),
                    'confidence': self._normalize_confidence(data.get('confidence', 0.5)),
                    'evidence': data.get('evidence', []),
                    'raw_text': text,
                }
            except json.JSONDecodeError:
                pass
        return self._fallback_response(text)

    def _fallback_response(self, text: str) -> dict[str, Any]:
        lines = [line.strip('-• ') for line in text.splitlines() if line.strip()]
        summary = lines[0] if lines else text[:240]
        evidence = lines[:5]
        return {
            'summary': summary,
            'signals': evidence[:3],
            'risks': evidence[3:5],
            'confidence': 0.5,
            'evidence': evidence,
            'raw_text': text,
        }

    def _normalize_confidence(self, value: Any) -> float:
        try:
            confidence = float(value)
        except (TypeError, ValueError):
            return 0.5
        if confidence > 1:
            confidence = confidence / 100
        return max(0.0, min(confidence, 1.0))

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        prompt = self.build_prompt(context)
        response = self.analyzer.openai_api_call(symbol=context.get('stock_code', ''), message=prompt, instruction=self.analyzer.instruction)
        parsed = self.parse_response(response)
        parsed['role'] = self.role_name
        return parsed
