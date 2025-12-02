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
st.set_page_config(layout="wide", page_title="사방넷 솔루션 v5.0 (Format Master)")
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

def clean_numeric_value(val):
    if pd.isna(val) or val == "": return ""
    s_val = str(val)
    clean_str = re.sub(r'[^0-9.]', '', s_val)
    try: return float(clean_str) if '.' in clean_str else int(clean_str)
    except: return val

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 2. 사이드바 및 초기 설정
# -------------------------------------------------------------------------
st.title("💎 사방넷 대량등록 솔루션 v5.0 (서식 제어)")

worksheet = get_db_connection()
if not worksheet: st.stop()

mappings_db = load_mappings_from_db(worksheet)
vendor_list = list(mappings_db.keys())

with st.sidebar:
    st.header("🏢 거래처 설정")
    select_options = ["(신규 업체 등록)"] + vendor_list
    selected_vendor = st.selectbox("작업할 거래처를 선택하세요", select_options)
    
    final_vendor_name = ""
    saved_mapping = {}

    if selected_vendor == "(신규 업체 등록)":
        new_vendor_name = st.text_input("새 거래처명 입력", placeholder="예: 나이키 24FW")
        final_vendor_name = new_vendor_name
        st.info("💡 신규 업체의 매핑 규칙을 새로 설정합니다.")
    else:
        final_vendor_name = selected_vendor
        saved_mapping = mappings_db.get(final_vendor_name, {})
        st.success(f"✅ '{final_vendor_name}' 설정을 불러왔습니다.")

    st.divider()
    with st.expander("🛠️ 양식 파일 관리 (Admin)"):
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

# 3-1. 양식 파일 로드
with col1:
    st.subheader("1. 기준 양식 & 데이터")
    if os.path.exists(MASTER_TEMPLATE_PATH):
        try: df_target = pd.read_excel(MASTER_TEMPLATE_PATH)
        except Exception as e: st.error(f"양식 오류: {e}")
    else:
        st.warning("내장 양식 없음. 업로드 필요.")
        uploaded_template = st.file_uploader("양식 파일", type=['csv', 'xlsx'])
        if uploaded_template: df_target = pd.read_excel(uploaded_template)

    file_02 = st.file_uploader("매입처 파일(신상품)", type=['csv', 'xlsx'])

# 3-2. 매핑 및 서식 설정 로직
if df_target is not None and file_02 is not None:
    try:
        # 데이터 읽을 때부터 모든 데이터를 문자열(dtype=str)로 읽어서 '001' 유지 (1차 방어)
        if file_02.name.endswith('.csv'): 
            df_source = pd.read_csv(file_02, encoding='cp949', dtype=str)
        else: 
            df_source = pd.read_excel(file_02, dtype=str)

        target_columns = df_target.columns.tolist()
        source_columns = df_source.columns.tolist()

        with col2:
            st.subheader(f"3. 정밀 매핑: {final_vendor_name}")
            if not final_vendor_name:
                st.warning("👈 사이드바에서 거래처를 선택하세요.")
                st.stop()

            user_selections = {} # 최종 저장될 딕셔너리
            
            # 레이아웃 헤더
            h1, h2, h3, h4 = st.columns([2, 2, 1.2, 0.5])
            h1.markdown("**사방넷 항목**")
            h2.markdown("**매핑 소스 / 값**")
            h3.markdown("**표시 형식**") # New Feature
            
            with st.container(height=600):
                for target_col in target_columns:
                    c1, c2, c3, c4 = st.columns([2, 2, 1.2, 0.5])
                    
                    # 1. 항목명 표시
                    with c1:
                        display_text = target_col.replace("\n", " ")
                        if "[필수]" in display_text: st.markdown(f"**🔴 {display_text}**")
                        else: st.text(display_text)
                    
                    # 2. 저장된 설정 복원 (v5.0 호환성 처리)
                    # 저장된 데이터 구조: {"val": "...", "fmt": "..."}
                    # 구버전 데이터 구조: "..." (문자열)
                    
                    saved_entry = saved_mapping.get(target_col)
                    
                    default_idx = 0
                    direct_input_val = ""
                    match_type = ""
                    default_fmt_idx = 0 # 0: 일반, 1: 텍스트, 2: 숫자
                    
                    current_val_str = ""
                    current_fmt_str = "General"

                    # (A) 저장된 값이 있는 경우
                    if saved_entry:
                        # 신버전(Dict)인지 구버전(Str)인지 확인
                        if isinstance(saved_entry, dict):
                            current_val_str = saved_entry.get("val", "")
                            current_fmt_str = saved_entry.get("fmt", "General")
                        else:
                            current_val_str = saved_entry # 구버전 호환
                        
                        # 값 복원 로직
                        if current_val_str.startswith("FIXED::"):
                            default_idx = 1
                            direct_input_val = current_val_str.replace("FIXED::", "")
                            match_type = "✏️"
                        elif current_val_str in source_columns:
                            default_idx = source_columns.index(current_val_str) + 2
                            match_type = "💾"
                        
                        # 서식 복원 로직
                        if current_fmt_str == "@": default_fmt_idx = 1 # 텍스트
                        elif current_fmt_str == "#,##0": default_fmt_idx = 2 # 숫자
                    
                    # (B) 저장된 값이 없으면 스마트 매핑
                    else:
                        target_clean = normalize_header(target_col)
                        for idx, src_col in enumerate(source_columns):
                            src_clean = normalize_header(src_col)
                            if target_clean and (target_clean == src_clean or target_clean in src_clean):
                                default_idx = idx + 2
                                match_type = "🤖"
                                break
                    
                    # 3. 매핑 선택 (Selectbox)
                    final_map_val = None
                    with c2:
                        options = ["(매핑 안함)", "(직접입력)"] + source_columns
                        selected = st.selectbox(f"s_{target_col}", options, index=default_idx, key=f"sb_{target_col}", label_visibility="collapsed")
                        
                        if selected == "(직접입력)":
                            inp = st.text_input("값", value=direct_input_val, key=f"txt_{target_col}", label_visibility="collapsed")
                            final_map_val = f"FIXED::{inp}"
                        elif selected != "(매핑 안함)":
                            final_map_val = selected
                    
                    # 4. [NEW] 표시 형식 선택 (Selectbox)
                    final_fmt_val = "General"
                    with c3:
                        # 매핑이 선택된 경우에만 서식 활성화
                        if final_map_val:
                            fmt_options = ["일반", "텍스트(001유지)", "숫자(1,000)"]
                            fmt_selected = st.selectbox("fmt", fmt_options, index=default_fmt_idx, key=f"fmt_{target_col}", label_visibility="collapsed")
                            
                            if fmt_selected == "텍스트(001유지)": final_fmt_val = "@"
                            elif fmt_selected == "숫자(1,000)": final_fmt_val = "#,##0"
                    
                    # 5. 상태 아이콘
                    with c4:
                        if match_type: st.text(match_type)
                    
                    # 6. 최종 딕셔너리에 저장 (값 + 서식)
                    if final_map_val:
                        user_selections[target_col] = {
                            "val": final_map_val,
                            "fmt": final_fmt_val
                        }

            if st.button("설정 저장 (Cloud DB)"):
                with st.spinner("저장 중..."):
                    if save_mapping_to_db(worksheet, final_vendor_name, user_selections):
                        st.toast(f"'{final_vendor_name}' 설정(서식 포함) 저장 완료!", icon="✅")
                        st.cache_resource.clear()
                    else: st.error("저장 실패")

        st.divider()
        st.subheader("4. 최종 변환 및 다운로드")
        
        if st.button("데이터 변환 실행"):
            with st.spinner('서식 적용 및 변환 중...'):
                result_df = pd.DataFrame(columns=target_columns)
                row_count = len(df_source)
                
                # 서식 정보를 나중에 쓰기 위해 별도 저장
                col_formats = {} 
                
                for target_col, setting in user_selections.items():
                    # setting은 이제 dict입니다. {"val":..., "fmt":...}
                    map_val = setting["val"]
                    fmt_val = setting["fmt"]
                    col_formats[target_col] = fmt_val # 서식 기억
                    
                    # 값 주입 로직
                    if map_val.startswith("FIXED::"):
                        val = map_val.replace("FIXED::", "")
                        result_df[target_col] = [val] * row_count
                    else:
                        raw_data = df_source[map_val]
                        
                        # 텍스트(@) 형식이면 -> 무조건 문자열로 변환하여 보존
                        if fmt_val == "@":
                            result_df[target_col] = raw_data.astype(str)
                        # 숫자(#,##0) 형식이면 -> 클리닝 수행
                        elif fmt_val == "#,##0":
                             result_df[target_col] = raw_data.apply(clean_numeric_value)
                        # 일반이면 -> 있는 그대로
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

                # -----------------------------------------------------------
                # [Expert Touch] XlsxWriter 엔진을 이용한 정밀 서식 제어
                # -----------------------------------------------------------
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    result_df.to_excel(writer, index=False, sheet_name='Sheet1')
                    workbook = writer.book
                    worksheet_xls = writer.sheets['Sheet1']
                    
                    # 서식 객체 생성
                    fmt_text = workbook.add_format({'num_format': '@'})     # 텍스트
                    fmt_num = workbook.add_format({'num_format': '#,##0'})  # 숫자(천단위)
                    
                    for i, col in enumerate(result_df.columns):
                        # 1. 컬럼 너비 자동 조정
                        col_str = str(col)
                        try: max_len = result_df[col].astype(str).map(len).max()
                        except: max_len = 0
                        width = min(max(len(col_str), max_len) + 2, 50)
                        
                        # 2. 사용자 지정 서식 적용
                        cell_format = None
                        user_fmt = col_formats.get(col, "General")
                        
                        if user_fmt == "@":
                            cell_format = fmt_text
                        elif user_fmt == "#,##0":
                            cell_format = fmt_num
                        
                        # 너비와 서식을 동시에 적용
                        worksheet_xls.set_column(i, i, width, cell_format)
                        
                output.seek(0)
                file_name = f"{final_vendor_name}_사방넷등록_{len(result_df)}건.xlsx"
                st.download_button("📥 결과 파일 다운로드", output, file_name)

    except Exception as e:
        st.error(f"처리 중 오류 발생: {e}")
