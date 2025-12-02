import streamlit as st
import pandas as pd
import json
import io
import re
import os
import gspread
from google.oauth2.service_account import Credentials

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 1. 시스템 설정 및 리소스 연결
# -------------------------------------------------------------------------
st.set_page_config(layout="wide", page_title="사방넷 솔루션 v4.0 (Pro)")

# 내장 템플릿 파일명 정의
MASTER_TEMPLATE_PATH = "master_template.xlsx"

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
        st.error(f"⚠️ DB 연결 실패: {e}")
        return None

def load_mappings_from_db(worksheet):
    if worksheet is None: return {}
    try:
        data = worksheet.get_all_records()
        mapping_dict = {}
        for row in data:
            vendor = row.get('Vendor')
            mapping_json = row.get('MappingData')
            if vendor and mapping_json:
                try: mapping_dict[vendor] = json.loads(mapping_json)
                except: continue
        return mapping_dict
    except Exception: return {}

def save_mapping_to_db(worksheet, vendor, mapping_data):
    if worksheet is None: return False
    try:
        cell = worksheet.find(vendor)
        json_str = json.dumps(mapping_data, ensure_ascii=False)
        if cell: worksheet.update_cell(cell.row, 2, json_str)
        else: worksheet.append_row([vendor, json_str])
        return True
    except Exception as e: return False

def normalize_header(header):
    header = re.sub(r'\[.*?\]', '', str(header))
    return re.sub(r'[^가-힣a-zA-Z0-9]', '', header).lower()

# [개선사항 1] 숫자 컬럼 세정 함수 (쉼표, 원 제거)
def clean_numeric_value(val):
    if pd.isna(val) or val == "": return ""
    s_val = str(val)
    # 숫자와 점(.)을 제외한 모든 문자 제거 (예: "10,000원" -> "10000")
    clean_str = re.sub(r'[^0-9.]', '', s_val)
    try:
        return float(clean_str) if '.' in clean_str else int(clean_str)
    except:
        return val # 변환 실패 시 원본 유지

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 2. 사이드바 및 초기 설정 (거래처 선택)
# -------------------------------------------------------------------------
st.title("🚀가울 사방넷 대량등록 솔루션 v4.0 (Pro)")

worksheet = get_db_connection()
if not worksheet: st.stop()

# DB에서 매핑 정보 로드
mappings_db = load_mappings_from_db(worksheet)
vendor_list = list(mappings_db.keys())

# 사이드바: 거래처 선택 및 관리
with st.sidebar:
    st.header("🏢 거래처 설정")
    
    # 거래처 선택 로직
    select_options = ["(신규 업체 등록)"] + vendor_list
    selected_vendor = st.selectbox("작업할 거래처를 선택하세요", select_options)
    
    final_vendor_name = ""
    saved_mapping = {}

    if selected_vendor == "(신규 업체 등록)":
        new_vendor_name = st.text_input("새 거래처명 입력", placeholder="예: 나이키 시즌2")
        final_vendor_name = new_vendor_name
        st.info("💡 신규 업체의 매핑 규칙을 새로 설정합니다.")
    else:
        final_vendor_name = selected_vendor
        saved_mapping = mappings_db.get(final_vendor_name, {})
        st.success(f"✅ '{final_vendor_name}' 설정을 불러왔습니다.")

    st.divider()
    
    # [개선사항 2] 템플릿 관리 기능 (Admin)
    with st.expander("🛠️ 양식 파일 관리 (Admin)"):
        st.write("기본 양식(master_template.xlsx) 업데이트")
        new_template = st.file_uploader("새 양식 파일", type=['xlsx', 'csv'])
        if new_template and st.button("양식 덮어쓰기"):
            with open(MASTER_TEMPLATE_PATH, "wb") as f:
                f.write(new_template.getbuffer())
            st.success("양식이 업데이트 되었습니다! (재시작 필요)")

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 3. 메인 UI 및 로직
# -------------------------------------------------------------------------
col1, col2 = st.columns([1, 2])

df_target = None
df_source = None

# 3-1. 양식 파일 로드 (자동)
with col1:
    st.subheader("1. 기준 양식 (Template)")
    
    # 로컬에 파일이 있는지 확인
    if os.path.exists(MASTER_TEMPLATE_PATH):
        st.info(f"📄 내장된 양식 사용 중: {MASTER_TEMPLATE_PATH}")
        try:
            df_target = pd.read_excel(MASTER_TEMPLATE_PATH)
        except Exception as e:
            st.error(f"양식 파일 오류: {e}")
    else:
        st.warning("⚠️ 내장 양식 파일이 없습니다. 업로드해주세요.")
        uploaded_template = st.file_uploader("양식 파일 업로드", type=['csv', 'xlsx'])
        if uploaded_template:
            if uploaded_template.name.endswith('.csv'):
                df_target = pd.read_csv(uploaded_template, encoding='cp949')
            else:
                df_target = pd.read_excel(uploaded_template)

    st.subheader("2. 신상품 데이터 (Source)")
    file_02 = st.file_uploader("매입처 파일 업로드", type=['csv', 'xlsx'])

# 3-2. 매핑 및 변환 로직
if df_target is not None and file_02 is not None:
    try:
        # 소스 파일 읽기
        if file_02.name.endswith('.csv'): df_source = pd.read_csv(file_02, encoding='cp949')
        else: df_source = pd.read_excel(file_02)

        target_columns = df_target.columns.tolist()
        source_columns = df_source.columns.tolist()

        with col2:
            st.subheader(f"3. 매핑 설정: {final_vendor_name}")
            
            if not final_vendor_name:
                st.warning("👈 왼쪽 사이드바에서 거래처명을 먼저 입력해주세요.")
                st.stop()

            user_selections = {}
            
            with st.container(height=600):
                for target_col in target_columns:
                    c1, c2, c3 = st.columns([2, 2, 0.5])
                    
                    with c1:
                        display_text = target_col.replace("\n", " ")
                        if "[필수]" in display_text: st.markdown(f"**🔴 {display_text}**")
                        else: st.text(display_text)
                    
                    # 매핑 기본값 로직
                    default_idx = 0
                    direct_input_val = ""
                    match_type = ""
                    
                    saved_val = saved_mapping.get(target_col)
                    
                    if saved_val:
                        if saved_val.startswith("FIXED::"):
                            default_idx = 1
                            direct_input_val = saved_val.replace("FIXED::", "")
                            match_type = "✏️"
                        elif saved_val in source_columns:
                            default_idx = source_columns.index(saved_val) + 2
                            match_type = "💾"
                    else:
                        target_clean = normalize_header(target_col)
                        for idx, src_col in enumerate(source_columns):
                            src_clean = normalize_header(src_col)
                            if target_clean and (target_clean == src_clean or target_clean in src_clean):
                                default_idx = idx + 2
                                match_type = "🤖"
                                break
                    
                    with c2:
                        options = ["(매핑 안함)", "(직접입력)"] + source_columns
                        selected = st.selectbox(f"sel_{target_col}", options, index=default_idx, key=f"sb_{target_col}", label_visibility="collapsed")
                        
                        final_val = None
                        if selected == "(직접입력)":
                            inp = st.text_input("값", value=direct_input_val, key=f"txt_{target_col}", label_visibility="collapsed")
                            final_val = f"FIXED::{inp}"
                        elif selected != "(매핑 안함)":
                            final_val = selected
                        
                        if final_val: user_selections[target_col] = final_val

                    with c3:
                        if match_type: st.text(match_type)

            if st.button("현재 매핑 저장 (Cloud DB)"):
                with st.spinner("저장 중..."):
                    if save_mapping_to_db(worksheet, final_vendor_name, user_selections):
                        st.toast(f"'{final_vendor_name}' 설정 저장 완료!", icon="✅")
                        st.cache_resource.clear()
                    else: st.error("저장 실패")

        st.divider()
        st.subheader("4. 최종 변환 및 다운로드")
        
        if st.button("데이터 변환 실행"):
            with st.spinner('데이터 처리 및 클리닝 중...'):
                result_df = pd.DataFrame(columns=target_columns)
                row_count = len(df_source)
                
                for target_col, map_val in user_selections.items():
                    if map_val.startswith("FIXED::"):
                        # 고정값 할당
                        val = map_val.replace("FIXED::", "")
                        result_df[target_col] = [val] * row_count
                    else:
                        # 데이터 매핑 및 [개선사항 1] 숫자 클리닝 적용
                        raw_data = df_source[map_val]
                        
                        # 가격 관련 컬럼인 경우 자동 정제
                        if any(keyword in target_col for keyword in ["판매가", "원가", "가격", "TAG가"]):
                            result_df[target_col] = raw_data.apply(clean_numeric_value)
                        else:
                            result_df[target_col] = raw_data
                
                result_df = result_df.fillna("")
                
                # Validation
                errs = []
                for col in target_columns:
                    if "[필수]" in col:
                        empty_check = (result_df[col] == "") | (result_df[col].isna())
                        if empty_check.sum() > 0: errs.append(f"⚠️ **{col}**: {empty_check.sum()}건 누락")
                
                if errs:
                    st.error(f"필수값 오류 {len(errs)}건")
                    for e in errs: st.write(e)
                else:
                    st.success("✅ 무결성 검증 통과!")

                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    result_df.to_excel(writer, index=False)
                    ws = writer.sheets['Sheet1']
                    for i, col in enumerate(result_df.columns):
                        col_str = str(col)
                        try: max_len = result_df[col].astype(str).map(len).max()
                        except: max_len = 0
                        ws.set_column(i, i, min(max(len(col_str), max_len) + 2, 40))
                        
                output.seek(0)
                file_name = f"{final_vendor_name}_사방넷등록_{len(result_df)}건.xlsx"
                st.download_button("📥 결과 파일 다운로드", output, file_name)

    except Exception as e:
        st.error(f"처리 중 오류 발생: {e}")

