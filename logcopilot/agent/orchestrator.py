from __future__ import annotations

from typing import TypedDict

from langchain_core.messages import HumanMessage, SystemMessage


SYSTEM_PROMPT = """Ты LogCopilot Agent.
Отвечай кратко, по-русски и по делу.
Ты работаешь только по уже собранному контексту из обработанных логов.
Не выдумывай данные и не придумывай метрики, которых нет в контексте.
Если данных недостаточно, прямо скажи об этом.
"""


class AgentState(TypedDict):
    question: str
    context: str
    answer: str


def build_chat_model(
    model: str = "qwen/qwen3.5-9b",
    base_url: str = "http://127.0.0.1:1234/v1",
    api_key: str = "lm-studio",
    temperature: float = 0.0,
):
    try:
        from langchain_openai import ChatOpenAI
    except ImportError as exc:
        raise RuntimeError(
            "Chat model requires langchain-openai. "
            "Install optional agent dependencies before using logcopilot.agent."
        ) from exc

    return ChatOpenAI(
        model=model,
        base_url=base_url,
        api_key=api_key,
        temperature=temperature,
    )


def build_agent(
    run_id: str,
    db_path: str = "out/logcopilot.sqlite",
    model: str = "qwen/qwen3.5-9b",
    base_url: str = "http://127.0.0.1:1234/v1",
    api_key: str = "lm-studio",
    temperature: float = 0.0,
):
    try:
        from langgraph.graph import END, StateGraph
    except ImportError as exc:
        raise RuntimeError(
            "Agent orchestration requires langchain/langgraph. "
            "Install optional agent dependencies before using logcopilot.agent."
        ) from exc

    llm = build_chat_model(
        model=model,
        base_url=base_url,
        api_key=api_key,
        temperature=temperature,
    )

    def gather_context(state: AgentState) -> AgentState:
        from .chat import answer_question as answer_question_rule

        context = answer_question_rule(run_id, state["question"], db_path=db_path)
        return {
            "question": state["question"],
            "context": context,
            "answer": "",
        }

    def answer(state: AgentState) -> AgentState:
        response = llm.invoke(
            [
                SystemMessage(content=SYSTEM_PROMPT),
                HumanMessage(
                    content=(
                        f"Вопрос пользователя:\n{state['question']}\n\n"
                        f"Контекст из tools:\n{state['context']}\n\n"
                        "Сформируй короткий полезный ответ."
                    )
                ),
            ]
        )
        content = response.content
        return {
            "question": state["question"],
            "context": state["context"],
            "answer": str(content),
        }

    graph = StateGraph(AgentState)
    graph.add_node("gather_context", gather_context)
    graph.add_node("answer", answer)
    graph.set_entry_point("gather_context")
    graph.add_edge("gather_context", "answer")
    graph.add_edge("answer", END)
    return graph.compile()
