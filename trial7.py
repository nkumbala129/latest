import streamlit as st
import json
import re
import requests
import snowflake.connector
import pandas as pd
from snowflake.snowpark import Session
from typing import Any, Dict, List, Optional, Tuple

# Snowflake/Cortex Configuration
HOST = "GNB14769.snowflakecomputing.com"
DATABASE = "CORTEX_SEARCH_TUTORIAL_DB"
SCHEMA = "PUBLIC"
STAGE = "CC_STAGE"
API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000  # in milliseconds
CORTEX_SEARCH_SERVICES = "CORTEX_SEARCH_TUTORIAL_DB.PUBLIC.BAYREN2"

# Semantic model options
SEMANTIC_MODEL_OPTIONS = {
    "CMP": '@"CORTEX_SEARCH_TUTORIAL_DB"."PUBLIC"."CMP_STAGE"/cmp 1 copy.yaml',
    "MFP": '@"CORTEX_SEARCH_TUTORIAL_DB"."PUBLIC"."MULTIFAMILYSTAGE"/multifamily.yaml',
    "CC": '@"CORTEX_SEARCH_TUTORIAL_DB"."PUBLIC"."CC_STAGE"/Climate_Career_Final_SM_Draft.yaml',
    "WUSaves": '@"CORTEX_SEARCH_TUTORIAL_DB"."PUBLIC"."WUSAVE_STAGE"/water_upgrades_save.yaml',
    "SF": '@"CORTEX_SEARCH_TUTORIAL_DB"."PUBLIC"."SF_STAGE"/single_family_sm.yaml',
    "GL": '@"CORTEX_TUTORIAL_DB"."PUBLIC"."GL_STAGE"/gl_projects.yaml',
}

# Streamlit Page Config
st.set_page_config(
    page_title="welcome to Cortex AI Assistant",
    layout="centered",
    initial_sidebar_state="auto"
)

# Initialize session state
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = ""
    st.session_state.password = ""
    st.session_state.CONN = None
    st.session_state.snowpark_session = None
if "debug_mode" not in st.session_state:
    st.session_state.debug_mode = False

# Hide Streamlit branding
st.markdown("""
<style>
#MainMenu, header, footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# Authentication logic
if not st.session_state.authenticated:
    st.title("Welcome to Snowflake Cortex AI ")
    st.markdown("Please login to interact with your data")

    st.session_state.username = st.text_input("Enter Snowflake Username:", value=st.session_state.username)
    st.session_state.password = st.text_input("Enter Password:", type="password")

    if st.button("Login"):
        try:
            conn = snowflake.connector.connect(
                user=st.session_state.username,
                password=st.session_state.password,
                account="GNB14769",
                host=HOST,
                port=443,
                warehouse="CORTEX_SEARCH_TUTORIAL_WH",
                role="DEV_BR_CORTEX_AI_ROLE",
                database=DATABASE,
                schema=SCHEMA,
            )
            st.session_state.CONN = conn

            snowpark_session = Session.builder.configs({
                "connection": conn
            }).create()
            st.session_state.snowpark_session = snowpark_session

            with conn.cursor() as cur:
                cur.execute(f"USE DATABASE {DATABASE}")
                cur.execute(f"USE SCHEMA {SCHEMA}")
                cur.execute("ALTER SESSION SET TIMEZONE = 'UTC'")
                cur.execute("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE")

            st.session_state.authenticated = True
            st.success("Authentication successful! Redirecting...")
            st.rerun()

        except Exception as e:
            st.error(f"Authentication failed: {e}")
else:
    session = st.session_state.snowpark_session

    # Utility Functions
    def run_snowflake_query(query):
        try:
            if not query:
                st.warning("⚠️ No SQL query generated.")
                return None
            df = session.sql(query)
            data = df.collect()
            if not data:
                return None
            columns = df.schema.names
            result_df = pd.DataFrame(data, columns=columns)
            return result_df
        except Exception as e:
            st.error(f"❌ SQL Execution Error: {str(e)}")
            return None

    def is_structured_query(query: str):
        structured_patterns = [
            r'\b(select|from|where|group by|order by|join|sum|count|avg|max|min)\b',
            r'\b(total|revenue|sales|profit|projects|county|jurisdiction|month|year|energy savings)\b'
        ]
        return any(re.search(pattern, query.lower()) for pattern in structured_patterns)

    def is_complete_query(query: str):
        complete_patterns = [r'\b(generate|write|create|describe|explain)\b']
        return any(re.search(pattern, query.lower()) for pattern in complete_patterns)

    def is_summarize_query(query: str):
        summarize_patterns = [r'\b(summarize|summary|condense)\b']
        return any(re.search(pattern, query.lower()) for pattern in summarize_patterns)

    def complete(prompt, model="mistral-large"):
        try:
            prompt = prompt.replace("'", "\\'")
            query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt}') AS response"
            result = session.sql(query).collect()
            return result[0]["RESPONSE"]
        except Exception as e:
            st.error(f"❌ COMPLETE Function Error: {str(e)}")
            return None

    def summarize(text):
        try:
            text = text.replace("'", "\\'")
            query = f"SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{text}') AS summary"
            result = session.sql(query).collect()
            return result[0]["SUMMARY"]
        except Exception as e:
            st.error(f"❌ SUMMARIZE Function Error: {str(e)}")
            return None

    def parse_sse_response(response_text: str) -> List[Dict]:
        """Parse SSE response into a list of JSON objects."""
        events = []
        lines = response_text.strip().split("\n")
        current_event = {}
        for line in lines:
            if line.startswith("event:"):
                current_event["event"] = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                data_str = line.split(":", 1)[1].strip()
                if data_str != "[DONE]":  # Skip the [DONE] marker
                    try:
                        data_json = json.loads(data_str)
                        current_event["data"] = data_json
                        events.append(current_event)
                        current_event = {}  # Reset for next event
                    except json.JSONDecodeError as e:
                        st.error(f"❌ Failed to parse SSE data: {str(e)} - Data: {data_str}")
        return events

    def snowflake_api_call(query: str, is_structured: bool = False, semantic_model: str = SEMANTIC_MODEL_OPTIONS["CMP"]):
        payload = {
            "model": "mistral-large",
            "messages": [{"role": "user", "content": [{"type": "text", "text": query}]}],
            "tools": []
        }
        if is_structured:
            payload["tools"].append({"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "analyst1"}})
            payload["tool_resources"] = {"analyst1": {"semantic_model_file": semantic_model}}
        else:
            payload["tools"].append({"tool_spec": {"type": "cortex_search", "name": "search1"}})
            payload["tool_resources"] = {"search1": {"name": CORTEX_SEARCH_SERVICES, "max_results": 1}}

        try:
            resp = requests.post(
                url=f"https://{HOST}{API_ENDPOINT}",
                json=payload,
                headers={
                    "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
                    "Content-Type": "application/json",
                },
                timeout=API_TIMEOUT // 1000
            )
            if st.session_state.debug_mode:  # Show debug info only if toggle is enabled
                st.write(f"API Response Status: {resp.status_code}")
                st.write(f"API Raw Response: {resp.text}")
            if resp.status_code < 400:
                if not resp.text.strip():
                    st.error("❌ API returned an empty response.")
                    return None
                return parse_sse_response(resp.text)
            else:
                raise Exception(f"Failed request with status {resp.status_code}: {resp.text}")
        except Exception as e:
            st.error(f"❌ API Request Failed: {str(e)}")
            return None

    def summarize_unstructured_answer(answer):
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|")\s', answer)
        return "\n".join(f"• {sent.strip()}" for sent in sentences[:6])

    def process_sse_response(response, is_structured):
        sql = ""
        search_results = []
        if not response:
            return sql, search_results
        try:
            for event in response:
                if event.get("event") == "message.delta" and "data" in event:
                    delta = event["data"].get("delta", {})
                    content = delta.get("content", [])
                    for item in content:
                        if item.get("type") == "tool_results":
                            tool_results = item.get("tool_results", {})
                            if "content" in tool_results:
                                for result in tool_results["content"]:
                                    if result.get("type") == "json":
                                        result_data = result.get("json", {})
                                        if is_structured and "sql" in result_data:
                                            sql = result_data.get("sql", "")
                                        elif not is_structured and "searchResults" in result_data:
                                            search_results = [sr["text"] for sr in result_data["searchResults"]]
        except Exception as e:
            st.error(f"❌ Error Processing Response: {str(e)}")
        return sql.strip(), search_results

    def generate_explanatory_summary(sql_query, results):
        try:
            results_text = "\n".join([str(row) for row in results])
            prompt = f"Provide a brief, 5-6 line non-technical summary of this result: {results_text} based on SQL query: {sql_query}."
            summary = complete(prompt)
            if summary:
                return "\n".join(summary.split("\n")[:3])
            else:
                return "⚠️ Unable to generate a concise summary."
        except Exception as e:
            return f"⚠️ Summary generation failed: {str(e)}"

    # UI Logic
    with st.sidebar:
        st.markdown("""
        <style>
        [data-testid="stSidebar"] [data-testid="stButton"] > button {
            background-color: #29B5E8 !important;
            color: white !important;
            font-weight: bold !important;
            width: 100% !important;
            border-radius: 0px !important;
            margin: 0 !important;
            border: none !important;
            padding: 0.5rem 1rem !important;
        }
        </style>
        """, unsafe_allow_html=True)

        logo_container = st.container()
        button_container = st.container()
        about_container = st.container()
        help_container = st.container()

        with logo_container:
            logo_url = "https://www.snowflake.com/wp-content/themes/snowflake/assets/img/logo-blue.svg"
            st.image(logo_url, width=200)

        with button_container:
            st.session_state.debug_mode = st.checkbox("Enable Debug Mode", value=st.session_state.debug_mode)

        with about_container:
            st.markdown("### About")
            st.write(
                "This application uses **Snowflake Cortex Analyst** to interpret "
                "your natural language questions and generate data insights. "
                "Simply ask a question below to see relevant answers and visualizations."
            )

        with help_container:
            st.markdown("### Help & Documentation")
            st.write(
                "- [User Guide](https://docs.snowflake.com/en/guides-overview-ai-features)  \n"
                "- [Snowflake Cortex Analyst Docs](https://docs.snowflake.com/)  \n"
                "- [Contact Support](https://www.snowflake.com/en/support/)"
            )

    st.title("🤖 Cortex AI Assistant")

    selected_model_name = st.sidebar.selectbox("Select Semantic Model", list(SEMANTIC_MODEL_OPTIONS.keys()), index=2)  # Default to "CC"
    selected_semantic_model = SEMANTIC_MODEL_OPTIONS[selected_model_name]
    
    semantic_model_filename = selected_semantic_model.split("/")[-1]
    st.markdown(f"Semantic Model: `{semantic_model_filename}`")

    st.sidebar.subheader("Sample Questions")
    sample_questions = [
        "What is BayREN?",
        "what is codes and standards program",
        "Give me all 6 programs names",
        "Show total energy savings by county.",
        "how many active projects are there in multi family program"
    ]
    query = st.chat_input("Ask your question...")

    for sample in sample_questions:
        if st.sidebar.button(sample, key=sample):
            query = sample

    if query:
        with st.chat_message("user"):
            st.markdown(query)
        with st.chat_message("assistant"):
            with st.spinner("Cool..Fetching the data... 🤖"):
                is_structured = is_structured_query(query)
                is_complete = is_complete_query(query)
                is_summarize = is_summarize_query(query)

                if is_complete:
                    response = complete(query)
                    if response:
                        st.markdown("**✍️ Generated Response:**")
                        st.write(response)
                    else:
                        st.warning("⚠️ Failed to generate a response.")
                elif is_summarize:
                    summary = summarize(query)
                    if summary:
                        st.markdown("**📝 Summary:**")
                        st.write(summary)
                    else:
                        st.warning("⚠️ Failed to generate a summary.")
                elif is_structured:
                    response = snowflake_api_call(query, is_structured=True, semantic_model=selected_semantic_model)
                    sql, _ = process_sse_response(response, is_structured=True)
                    if sql:
                        results = run_snowflake_query(sql)
                        if results is not None and not results.empty:
                            summary = generate_explanatory_summary(sql, results)
                            st.markdown("**🛠️ Generated SQL Query:**")
                            st.code(sql, language="sql")
                            st.markdown("**📝 Summary of Query Results:**")
                            st.write(summary)
                            st.markdown(f"**📊 Query Results ({len(results)} rows):**")
                            st.dataframe(results)
                        else:
                            st.warning("⚠️ No data found.")
                    else:
                        st.warning("⚠️ No SQL generated.")
                else:
                    response = snowflake_api_call(query, is_structured=False, semantic_model=selected_semantic_model)
                    _, search_results = process_sse_response(response, is_structured=False)
                    if search_results:
                        raw_result = search_results[0]
                        summary = summarize(raw_result)
                        if summary:
                            st.markdown("**🔍 Here is the Answer to your query:**")
                            st.write(summary)
                            last_sentence = summary.split(".")[-2] if "." in summary else summary
                            st.success(f"✅ Key Insight: {last_sentence.strip()}")
                        else:
                            st.markdown("**🔍 Key Information (Unsummarized):**")
                            st.write(summarize_unstructured_answer(raw_result))
                    else:
                        st.warning("⚠️ No relevant search results found.")
