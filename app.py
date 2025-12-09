import streamlit as st
import pandas as pd
import json
import io
import re
import os
import gspread
from google.oauth2.service_account import Credentials
import time

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 1. 시스템 설정
# -------------------------------------------------------------------------
st.set_page_config(layout="wide", page_title="사방넷 솔루션 v5.3 (Persistence)")
MASTER_TEMPLATE_PATH = "master_template.xlsx"

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 2. DB 연결 및 데이터 관리 (핵심 수정)
# -------------------------------------------------------------------------

# 연결 객체는 영구적으로 캐싱 (리소스 절약)
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

# [핵심] 데이터 로드 함수에 TTL(유효기간) 설정하지 않음 -> 대신 명시적 갱신 사용
# 서버가 재시작되면 이 함수가 무조건 다시 실행되어 DB에서 최신값을 가져옴
def fetch_all_mappings(worksheet):
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
# [웹프로그래밍 전문가] 3. 초기화 및 사이드바 (서버 재부팅 대응 로직)
# -------------------------------------------------------------------------
st.title("💎 사방넷 대량등록 솔루션 v5.3 (데이터 영구보존)")

worksheet = get_db_connection()
if not worksheet: st.stop()

# [수정됨] 세션 상태에 의존하지 않고, 매번 DB에서 최신 데이터를 가져옴
# 이렇게 하면 서버가 재부팅되어도 DB에 있는 목록을 다시 불러옵니다.
if "cached_mappings" not in st.session_state:
    with st.spinner("서버 시작: DB에서 데이터 불러오는 중..."):
        st.session_state.cached_mappings = fetch_all_mappings(worksheet)

mappings_db = st.session_state.cached_mappings
vendor_list = sorted(list(mappings_db.keys())) # DB 기반 리스트

with st.sidebar:
    st.header("🏢 거래처 설정")
    
    # [새로고침 버튼] DB 강제 동기화 기능
    if st.button("🔄 DB 목록 새로고침"):
        st.session_state.cached_mappings = fetch_all_mappings(worksheet)
        st.rerun()

    select_options = ["(신규 업체 등록)"] + vendor_list
    
    # 선택 인덱스 관리 (서버 재부팅 시 0번으로 초기화되는 것은 정상 동작)
    if "selected_idx" not in st.session_state:
        st.session_state.selected_idx = 0
    
    # 인덱스 범위 안전장치
    if st.session_state.selected_idx >= len(select_options):
        st.session_state.selected_idx = 0

    selected_vendor = st.selectbox(
        "작업할 거래처를 선택하세요", 
        select_options,
        index=st.session_state.selected_idx,
        key="vendor_select_box"
    )
    
    # 선택값 동기화
    final_vendor_name = ""
    saved_mapping = {}

    if selected_vendor == "(신규 업체 등록)":
        new_vendor_name = st.text_input("새 거래처명 입력", placeholder="예: 나이키 24FW")
        final_vendor_name = new_vendor_name
        st.info("새로운 거래처를 DB에 등록합니다.")
    else:
        final_vendor_name = selected_vendor
        saved_mapping = mappings_db.get(final_vendor_name, {})
        st.success(f"데이터 로드 완료: {final_vendor_name}")

    st.divider()
    with st.expander("🛠️ 양식 파일 관리 (Admin)"):
        new_template = st.file_uploader("새 양식 파일", type=['xlsx', 'csv'])
        if new_template and st.button("양식 덮어쓰기"):
            with open(MASTER_TEMPLATE_PATH, "wb") as f:
                f.write(new_template.getbuffer())
            st.success("양식 업데이트 완료")
            time.sleep(1)
            st.rerun()

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 4. 메인 로직
# -------------------------------------------------------------------------
col1, col2 = st.columns([1, 2])
df_target = None
df_source = None

# 4-1. 파일 로드
with col1:
    st.subheader("1. 기준 양식 & 데이터")
    if os.path.exists(MASTER_TEMPLATE_PATH):
        try: df_target = pd.read_excel(MASTER_TEMPLATE_PATH)
        except Exception as e: st.error(f"양식 오류: {e}")
    else:
        st.warning("내장 양식 없음")
        uploaded_template = st.file_uploader("양식 파일", type=['csv', 'xlsx'])
        if uploaded_template: df_target = pd.read_excel(uploaded_template)

    file_02 = st.file_uploader("매입처 파일(신상품)", type=['csv', 'xlsx'])

# 4-2. 매핑 및 변환
if df_target is not None and file_02 is not None:
    try:
        if file_02.name.endswith('.csv'): 
            df_source = pd.read_csv(file_02, encoding='cp949', dtype=str)
        else: 
            df_source = pd.read_excel(file_02, dtype=str)

        target_columns = df_target.columns.tolist()
        source_columns = df_source.columns.tolist()

        with col2:
            st.subheader(f"3. 정밀 매핑: {final_vendor_name}")
            if not final_vendor_name:
                st.warning("👈 거래처명을 입력하거나 선택해주세요.")
                st.stop()

            user_selections = {} 
            
            h1, h2, h3, h4 = st.columns([2, 2, 1.2, 0.5])
            h1.markdown("**사방넷 항목**")
            h2.markdown("**매핑 소스 / 값**")
            h3.markdown("**표시 형식**")
            
            with st.container(height=600):
                for target_col in target_columns:
                    c1, c2, c3, c4 = st.columns([2, 2, 1.2, 0.5])
                    with c1:
                        display_text = target_col.replace("\n", " ")
                        if "[필수]" in display_text: st.markdown(f"**🔴 {display_text}**")
                        else: st.text(display_text)
                    
                    # 값 복원
                    saved_entry = saved_mapping.get(target_col)
                    default_idx = 0
                    direct_input_val = ""
                    match_type = ""
                    default_fmt_idx = 0 
                    
                    if saved_entry:
                        current_val = saved_entry.get("val", "") if isinstance(saved_entry, dict) else saved_entry
                        current_fmt = saved_entry.get("fmt", "General") if isinstance(saved_entry, dict) else "General"
                        
                        if current_val.startswith("FIXED::"):
                            default_idx = 1
                            direct_input_val = current_val.replace("FIXED::", "")
                            match_type = "✏️"
                        elif current_val in source_columns:
                            default_idx = source_columns.index(current_val) + 2
                            match_type = "💾"
                        
                        if current_fmt == "@": default_fmt_idx = 1
                        elif current_fmt == "#,##0": default_fmt_idx = 2
                    else:
                        target_clean = normalize_header(target_col)
                        for idx, src_col in enumerate(source_columns):
                            src_clean = normalize_header(src_col)
                            if target_clean and (target_clean == src_clean or target_clean in src_clean):
                                default_idx = idx + 2
                                match_type = "🤖"
                                break
                    
                    # UI 렌더링
                    final_map_val = None
                    with c2:
                        opts = ["(매핑 안함)", "(직접입력)"] + source_columns
                        sel = st.selectbox(f"s_{target_col}", opts, index=default_idx, key=f"sb_{target_col}", label_visibility="collapsed")
                        if sel == "(직접입력)":
                            inp = st.text_input("값", value=direct_input_val, key=f"txt_{target_col}", label_visibility="collapsed")
                            final_map_val = f"FIXED::{inp}"
                        elif sel != "(매핑 안함)":
                            final_map_val = sel
                    
                    final_fmt = "General"
                    with c3:
                        if final_map_val:
                            f_opts = ["일반", "텍스트(001유지)", "숫자(1,000)"]
                            f_sel = st.selectbox("fmt", f_opts, index=default_fmt_idx, key=f"fmt_{target_col}", label_visibility="collapsed")
                            if f_sel == "텍스트(001유지)": final_fmt = "@"
                            elif f_sel == "숫자(1,000)": final_fmt = "#,##0"
                    
                    with c4:
                        if match_type: st.text(match_type)
                    
                    if final_map_val:
                        user_selections[target_col] = {"val": final_map_val, "fmt": final_fmt}

            # [수정됨] 저장 로직
            if st.button("설정 저장 (Cloud DB)"):
                with st.spinner("DB 저장 중..."):
                    if save_mapping_to_db(worksheet, final_vendor_name, user_selections):
                        # 1. DB 갱신 성공
                        # 2. 로컬 캐시(st.session_state)도 즉시 업데이트하여 Rerun시 반영되게 함
                        st.session_state.cached_mappings[final_vendor_name] = user_selections
                        
                        # 3. 새로 생성된 항목이라면 리스트 정렬 다시 필요
                        st.session_state.cached_vendor_list = sorted(list(st.session_state.cached_mappings.keys()))
                        
                        # 4. 선택 인덱스 조정 (방금 저장한 항목 선택)
                        new_vendor_list = ["(신규 업체 등록)"] + sorted(list(st.session_state.cached_mappings.keys()))
                        try:
                            st.session_state.selected_idx = new_vendor_list.index(final_vendor_name)
                        except:
                            st.session_state.selected_idx = 0

                        st.toast("저장 및 동기화 완료!", icon="✅")
                        time.sleep(1)
                        st.rerun()
                    else: 
                        st.error("저장 실패")

        st.divider()
        st.subheader("4. 최종 변환 및 다운로드")
        
        if st.button("데이터 변환 실행"):
            with st.spinner('변환 중...'):
                result_df = pd.DataFrame(columns=target_columns)
                row_count = len(df_source)
                col_formats = {}
                
                for t_col, setting in user_selections.items():
                    m_val = setting["val"]
                    f_val = setting["fmt"]
                    col_formats[t_col] = f_val
                    
                    if m_val.startswith("FIXED::"):
                        result_df[t_col] = [m_val.replace("FIXED::", "")] * row_count
                    else:
                        raw = df_source[m_val]
                        if f_val == "@": result_df[t_col] = raw.astype(str)
                        elif f_val == "#,##0": result_df[t_col] = raw.apply(clean_numeric_value)
                        else: result_df[t_col] = raw
                
                result_df = result_df.fillna("")
                
                # Validation
                errs = []
                for col in target_columns:
                    if "[필수]" in col:
                        if ((result_df[col] == "") | (result_df[col].isna())).sum() > 0:
                            errs.append(f"⚠️ **{col}** 누락")
                
                if errs:
                    st.error(f"필수값 오류 {len(errs)}건")
                    for e in errs: st.write(e)
                else:
                    st.success("✅ 무결성 검증 통과")

                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    result_df.to_excel(writer, index=False, sheet_name='Sheet1')
                    ws = writer.sheets['Sheet1']
                    fmt_txt = writer.book.add_format({'num_format': '@'})
                    fmt_num = writer.book.add_format({'num_format': '#,##0'})
                    
                    for i, col in enumerate(result_df.columns):
                        width = 20
                        try: width = min(max(len(str(col)), result_df[col].astype(str).map(len).max()) + 2, 50)
                        except: pass
                        
                        cf = None
                        uf = col_formats.get(col, "General")
                        if uf == "@": cf = fmt_txt
                        elif uf == "#,##0": cf = fmt_num
                        ws.set_column(i, i, width, cf)
                        
                output.seek(0)
                st.download_button("📥 결과 다운로드", output, f"{final_vendor_name}_완료.xlsx")

    except Exception as e:
        st.error(f"오류: {e}")
