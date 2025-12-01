import streamlit as st
import pandas as pd
import json
import io
import re
import gspread
from google.oauth2.service_account import Credentials

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 1. 환경 설정 및 DB 연결 (기존과 동일)
# -------------------------------------------------------------------------
st.set_page_config(layout="wide", page_title="사방넷 솔루션 v3.2 (Direct Input)")

@st.cache_resource
def get_db_connection():
    # ... (기존 DB 연결 코드 유지) ...
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    try:
        credentials_info = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(credentials_info, scopes=scope)
        client = gspread.authorize(creds)
        sheet_url = st.secrets["private_sheet_url"] 
        sheet = client.open_by_url(sheet_url)
        return sheet.sheet1
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        return None

def load_mappings_from_db(worksheet):
    # ... (기존 로드 함수 유지) ...
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
    # ... (기존 저장 함수 유지) ...
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

# -------------------------------------------------------------------------
# [웹프로그래밍 전문가] 2. 메인 로직 (직접입력 UI 추가됨)
# -------------------------------------------------------------------------
st.title("☁️ 사방넷 대량등록 솔루션 v3.2 (직접입력 기능)")

worksheet = get_db_connection()
if not worksheet: st.stop()

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
            st.subheader("2. 스마트 매핑 & 직접 입력")
            supplier_name = st.text_input("거래처명 (저장 Key)", placeholder="예: 나이키")
            
            mappings_db = load_mappings_from_db(worksheet)
            saved_mapping = mappings_db.get(supplier_name, {})
            
            if supplier_name and supplier_name in mappings_db:
                st.success(f"📂 설정을 불러왔습니다: '{supplier_name}'")

            st.markdown("---")
            
            user_selections = {}
            
            # [UI 개선] 스크롤 컨테이너
            with st.container(height=600):
                for target_col in target_columns:
                    c1, c2, c3 = st.columns([2, 2, 0.5])
                    
                    # 1. 라벨 (필수 표시)
                    with c1:
                        display_text = target_col.replace("\n", " ")
                        if "[필수]" in display_text:
                            st.markdown(f"**🔴 {display_text}**")
                        else:
                            st.text(display_text)
                    
                    # 2. 기본값 결정 로직
                    default_idx = 0     # 0: (매핑 안함)
                    saved_val = saved_mapping.get(target_col)
                    match_type = ""
                    direct_input_val = "" # 직접입력 시 복원할 값

                    # Case A: 저장된 값이 있을 때
                    if saved_val:
                        if saved_val.startswith("FIXED::"): # 직접입력값인 경우
                            default_idx = 1 # 1: (직접입력)
                            direct_input_val = saved_val.replace("FIXED::", "")
                            match_type = "✏️"
                        elif saved_val in source_columns: # 컬럼 매핑인 경우
                            default_idx = source_columns.index(saved_val) + 2 # +2: 매핑안함, 직접입력 다음
                            match_type = "💾"
                    
                    # Case B: 저장된 값이 없고 스마트 매핑 시도
                    else:
                        target_clean = normalize_header(target_col)
                        for idx, src_col in enumerate(source_columns):
                            src_clean = normalize_header(src_col)
                            if target_clean and (target_clean == src_clean or target_clean in src_clean):
                                default_idx = idx + 2
                                match_type = "🤖"
                                break
                    
                    # 3. 셀렉트박스 & 입력창 렌더링
                    with c2:
                        # 옵션 리스트: [매핑 안함, 직접입력, ...소스컬럼들...]
                        options = ["(매핑 안함)", "(직접입력)"] + source_columns
                        
                        selected_option = st.selectbox(
                            f"Sel_{target_col}", options, 
                            index=default_idx, 
                            key=f"sb_{target_col}", 
                            label_visibility="collapsed"
                        )
                        
                        final_value = None
                        
                        # (직접입력) 선택 시 텍스트 입력창 표시
                        if selected_option == "(직접입력)":
                            user_input = st.text_input(
                                "값 입력", 
                                value=direct_input_val, 
                                key=f"txt_{target_col}",
                                label_visibility="collapsed",
                                placeholder="고정값 입력"
                            )
                            # 내부 저장용 포맷: FIXED::값
                            final_value = f"FIXED::{user_input}"
                        elif selected_option != "(매핑 안함)":
                            final_value = selected_option
                        
                        # 결과 딕셔너리에 저장 (매핑 안함 제외)
                        if final_value:
                            user_selections[target_col] = final_value

                    with c3:
                        if match_type: st.text(match_type)

            if st.button("설정 저장 (Cloud DB)"):
                if not supplier_name:
                    st.error("거래처명을 입력해주세요.")
                else:
                    with st.spinner("저장 중..."):
                        if save_mapping_to_db(worksheet, supplier_name, user_selections):
                            st.toast("저장 완료!", icon="✅")
                            st.cache_resource.clear()
                        else: st.error("저장 실패")

        # ---------------------------------------------------------------------
        # [웹프로그래밍 전문가] 3. 데이터 변환 엔진 (FIXED 처리 추가)
        # ---------------------------------------------------------------------
        st.divider()
        st.subheader("3. 결과 생성")

        if st.button("데이터 변환 실행"):
            with st.spinner('데이터 생성 중...'):
                result_df = pd.DataFrame(columns=target_columns)
                
                # [핵심] 매핑 적용 로직
                # 소스 데이터의 행 수만큼 빈 DataFrame 준비 (안전한 방식)
                row_count = len(df_source)
                
                # 미리 행을 확보하지 않으면 단일값 할당 시 에러 가능성 있으므로
                # 우선 소스 데이터프레임의 인덱스를 기준으로 병합
                
                for target_col, map_val in user_selections.items():
                    if map_val.startswith("FIXED::"):
                        # 고정값 처리: 모든 행에 동일한 값 할당
                        fixed_value = map_val.replace("FIXED::", "")
                        result_df[target_col] = [fixed_value] * row_count
                    else:
                        # 컬럼 매핑 처리
                        result_df[target_col] = df_source[map_val]
                
                # 매핑되지 않은 나머지 컬럼은 NaN -> 빈 문자열 처리
                result_df = result_df.fillna("")
                
                # Validation (필수값 체크)
                errs = []
                for col in target_columns:
                    if "[필수]" in col:
                        # 빈 문자열("") 이거나 NaN인 경우 체크
                        empty_mask = (result_df[col] == "") | (result_df[col].isna())
                        if empty_mask.sum() > 0:
                            errs.append(f"⚠️ **{col}**: {empty_mask.sum()}건 누락")
                
                if errs:
                    st.error(f"필수값 누락 {len(errs)}건 발견!")
                    for e in errs: st.write(e)
                else:
                    st.success("✅ 무결성 검증 통과!")

                # 엑셀 다운로드
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    result_df.to_excel(writer, index=False)
                    ws = writer.sheets['Sheet1']
                    # 컬럼 너비 조정
                    for i, col in enumerate(result_df.columns):
                        col_str = str(col)
                        try: max_len = result_df[col].astype(str).map(len).max()
                        except: max_len = 0
                        ws.set_column(i, i, min(max(len(col_str), max_len) + 2, 40))
                        
                output.seek(0)
                file_prefix = supplier_name if supplier_name else "사방넷_변환완료"
                st.download_button("📥 결과 파일 다운로드", output, f"{file_prefix}.xlsx")

    except Exception as e:
        st.error(f"시스템 오류: {e}")
