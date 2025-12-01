import streamlit as st
import pandas as pd
import json
import io
import re
import gspread
from google.oauth2.service_account import Credentials

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 1. 환경 설정 및 DB 연결
# -------------------------------------------------------------------------
st.set_page_config(layout="wide", page_title="사방넷 솔루션 v3.1 (Fixed)")

# Google Sheets 연결 함수
@st.cache_resource
def get_db_connection():
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    try:
        credentials_info = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(credentials_info, scopes=scope)
        client = gspread.authorize(creds)
        sheet_url = st.secrets["private_sheet_url"] 
        sheet = client.open_by_url(sheet_url)
        return sheet.sheet1
    except Exception as e:
        st.error(f"DB 연결 실패: secrets 설정을 확인해주세요. ({e})")
        return None

# 매핑 데이터 로드
def load_mappings_from_db(worksheet):
    if worksheet is None: return {}
    try:
        data = worksheet.get_all_records()
        mapping_dict = {}
        for row in data:
            vendor = row.get('Vendor')
            mapping_json = row.get('MappingData')
            if vendor and mapping_json:
                try:
                    mapping_dict[vendor] = json.loads(mapping_json)
                except:
                    continue
        return mapping_dict
    except Exception:
        if worksheet.row_count == 0 or not worksheet.get_values():
            worksheet.append_row(['Vendor', 'MappingData'])
        return {}

# 매핑 데이터 저장
def save_mapping_to_db(worksheet, vendor, mapping_data):
    if worksheet is None: return False
    try:
        cell = worksheet.find(vendor)
        json_str = json.dumps(mapping_data, ensure_ascii=False)
        if cell:
            worksheet.update_cell(cell.row, 2, json_str)
        else:
            worksheet.append_row([vendor, json_str])
        return True
    except Exception as e:
        st.error(f"저장 중 오류 발생: {e}")
        return False

# 정규화 함수
def normalize_header(header):
    header = re.sub(r'\[.*?\]', '', str(header))
    return re.sub(r'[^가-힣a-zA-Z0-9]', '', header).lower()

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 2. 메인 로직
# -------------------------------------------------------------------------
st.title("☁️ 사방넷 대량등록 솔루션 v3.1 (Google DB)")

worksheet = get_db_connection()
if not worksheet:
    st.stop()

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("1. 파일 업로드")
    file_01 = st.file_uploader("01. 양식 파일 (Target)", type=['csv', 'xlsx'])
    file_02 = st.file_uploader("02. 데이터 파일 (Source)", type=['csv', 'xlsx'])

# 파일이 업로드되었을 때만 실행
if file_01 and file_02:
    # >>> try 블록 시작 <<<
    try:
        # 파일 읽기
        if file_01.name.endswith('.csv'): df_target = pd.read_csv(file_01, encoding='cp949')
        else: df_target = pd.read_excel(file_01)
            
        if file_02.name.endswith('.csv'): df_source = pd.read_csv(file_02, encoding='cp949')
        else: df_source = pd.read_excel(file_02)

        target_columns = df_target.columns.tolist()
        source_columns = df_source.columns.tolist()

        with col2:
            st.subheader("2. 스마트 컬럼 매핑 (DB Synced)")
            supplier_name = st.text_input("거래처명 (저장 Key)", placeholder="예: 나이키")
            
            mappings_db = load_mappings_from_db(worksheet)
            saved_mapping = mappings_db.get(supplier_name, {})
            
            if supplier_name and supplier_name in mappings_db:
                st.success(f"📂 Cloud DB: '{supplier_name}' 매핑 로드됨")

            st.markdown("---")
            
            user_selections = {}
            with st.container(height=600):
                for target_col in target_columns:
                    c1, c2, c3 = st.columns([2, 2, 0.5])
                    with c1:
                        display_text = target_col.replace("\n", " ")
                        if "[필수]" in display_text:
                            st.markdown(f"**🔴 {display_text}**")
                        else:
                            st.text(display_text)
                    
                    default_idx = 0
                    match_type = ""
                    
                    if saved_mapping.get(target_col) in source_columns:
                        default_idx = source_columns.index(saved_mapping[target_col]) + 1
                        match_type = "💾"
                    else:
                        target_clean = normalize_header(target_col)
                        for idx, src_col in enumerate(source_columns):
                            src_clean = normalize_header(src_col)
                            if target_clean and (target_clean == src_clean or target_clean in src_clean):
                                default_idx = idx + 1
                                match_type = "🤖"
                                break
                    
                    with c2:
                        selected = st.selectbox(
                            f"Select {target_col}", ["(매핑 안함)"] + source_columns, 
                            index=default_idx, key=f"map_{target_col}", label_visibility="collapsed"
                        )
                        if selected != "(매핑 안함)":
                            user_selections[target_col] = selected
                    with c3:
                        if match_type: st.text(match_type)

            if st.button("현재 매핑 Cloud DB에 저장"):
                if not supplier_name:
                    st.error("거래처명을 입력해주세요.")
                else:
                    with st.spinner("저장 중..."):
                        if save_mapping_to_db(worksheet, supplier_name, user_selections):
                            st.toast("저장 완료!", icon="✅")
                            st.cache_resource.clear()
                        else:
                            st.error("저장 실패")

        # ---------------------------------------------------------------------
        # [수정된 부분] 여기서부터 들여쓰기를 맞춰 try 블록 안에 포함시킴
        # ---------------------------------------------------------------------
        st.divider()
        st.subheader("3. 결과 생성")

        if st.button("데이터 변환 및 검증 실행"):
            with st.spinner('처리 중...'):
                result_df = pd.DataFrame(columns=target_columns)
                for t_col, s_col in user_selections.items():
                    result_df[t_col] = df_source[s_col]
                result_df = result_df.fillna("")
                
                # Validation
                errs = []
                for col in target_columns:
                    if "[필수]" in col:
                        empty_cnt = (result_df[col] == "").sum() + result_df[col].isna().sum()
                        if empty_cnt > 0: errs.append(f"⚠️ **{col}**: {empty_cnt}건 누락")
                
                if errs:
                    st.error("필수값 누락 발견!")
                    for e in errs: st.write(e)
                else:
                    st.success("무결성 검증 통과!")

                # Excel Output
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    result_df.to_excel(writer, index=False)
                    ws = writer.sheets['Sheet1']
                    for i, col in enumerate(result_df.columns):
                        # 컬럼 너비 조정 (간소화)
                        col_str = str(col)
                        try:
                            max_len = result_df[col].astype(str).map(len).max()
                            if pd.isna(max_len): max_len = 0
                        except: max_len = 0
                        ws.set_column(i, i, min(max(len(col_str), max_len) + 2, 40))
                        
                output.seek(0)
                file_prefix = supplier_name if supplier_name else "변환완료"
                st.download_button("📥 결과 파일 다운로드", output, f"{file_prefix}.xlsx")

    # >>> try 블록 종료, except 블록 연결 <<<
    except Exception as e:
        st.error(f"시스템 처리 중 오류 발생: {e}")
