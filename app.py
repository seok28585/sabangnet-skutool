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
st.set_page_config(layout="wide", page_title="사방넷 솔루션 v3.0 (Cloud DB)")

# Google Sheets 연결 함수 (캐싱을 통해 속도 최적화)
@st.cache_resource
def get_db_connection():
    # Streamlit Secrets에서 인증 정보 로드
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    
    # st.secrets가 있는 경우(배포/로컬 설정)와 없는 경우 예외처리
    try:
        credentials_info = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(credentials_info, scopes=scope)
        client = gspread.authorize(creds)
        
        # 시트 열기 (Secrets에 저장된 시트 URL 또는 ID 사용)
        sheet_url = st.secrets["private_sheet_url"] 
        sheet = client.open_by_url(sheet_url)
        return sheet.sheet1  # 첫 번째 시트 사용
    except Exception as e:
        st.error(f"DB 연결 실패: secrets 설정을 확인해주세요. ({e})")
        return None

# 매핑 데이터 로드 (Google Sheets -> Dict)
def load_mappings_from_db(worksheet):
    if worksheet is None: return {}
    try:
        # 모든 레코드 가져오기 (Expected columns: 'Vendor', 'MappingData')
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
        # 시트가 비어있거나 컬럼이 없는 초기 상태 처리
        if worksheet.row_count == 0 or not worksheet.get_values():
            worksheet.append_row(['Vendor', 'MappingData']) # 헤더 생성
        return {}

# 매핑 데이터 저장 (Dict -> Google Sheets Upsert)
def save_mapping_to_db(worksheet, vendor, mapping_data):
    if worksheet is None: return False
    try:
        # 기존 데이터 확인
        cell = worksheet.find(vendor)
        json_str = json.dumps(mapping_data, ensure_ascii=False)
        
        if cell:
            # 이미 존재하면 Update (Vendor 옆 칸인 B열 업데이트)
            worksheet.update_cell(cell.row, 2, json_str)
        else:
            # 없으면 Insert
            worksheet.append_row([vendor, json_str])
        return True
    except Exception as e:
        st.error(f"저장 중 오류 발생: {e}")
        return False

# 정규화 함수 (스마트 매핑용)
def normalize_header(header):
    header = re.sub(r'\[.*?\]', '', str(header))
    return re.sub(r'[^가-힣a-zA-Z0-9]', '', header).lower()

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 2. 메인 로직 시작
# -------------------------------------------------------------------------
st.title("☁️ 사방넷 대량등록 솔루션 v3.0 (Google DB 연동)")
st.markdown("""
> **System Info**: 매핑 규칙이 **Google Sheets**에 안전하게 저장됩니다.
> 동료들과 실시간으로 매핑 정보를 공유할 수 있습니다.
""")

# DB 연결 시도
worksheet = get_db_connection()
if not worksheet:
    st.stop() # DB 연결 안되면 중단

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("1. 파일 업로드")
    file_01 = st.file_uploader("01. 양식 파일 (Target)", type=['csv', 'xlsx'])
    file_02 = st.file_uploader("02. 데이터 파일 (Source)", type=['csv', 'xlsx'])

if file_01 and file_02:
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
            
            # DB에서 매핑 정보 로드
            mappings_db = load_mappings_from_db(worksheet)
            saved_mapping = mappings_db.get(supplier_name, {})
            
            if supplier_name and supplier_name in mappings_db:
                st.success(f"📂 Cloud DB: '{supplier_name}' 매핑 불러오기 성공!")

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
                    
                    # 1. DB 저장값 확인
                    if saved_mapping.get(target_col) in source_columns:
                        default_idx = source_columns.index(saved_mapping[target_col]) + 1
                        match_type = "💾"
                    # 2. 스마트 매핑
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
                    with st.spinner("Google Sheets에 저장 중..."):
                        if save_mapping_to_db(worksheet, supplier_name, user_selections):
                            st.toast(f"✅ '{supplier_name}' 저장 완료!", icon="☁️")
                            st.cache_resource.clear() # 캐시 갱신 (선택사항)
                        else:
                            st.error("저장 실패")

    # 변환 및 다운로드 로직 (이전과 동일하여 핵심만 유지)
    st.divider()
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
                    ws.set_column(i, i, 20) # 간략화된 너비 조정
            output.seek(0)
            
            st.download_button("📥 결과 파일 다운로드", output, f"{supplier_name}_완료.xlsx")

except Exception as e:
    st.error(f"오류: {e}")