import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime
import time
from io import BytesIO

def load_env(path=".env"):
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        pass

load_env()

def db_config():
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_NAME")
    port = os.getenv("DB_PORT")
    if not host or not user or password is None or not database:
        return None
    cfg = {
        "host": host,
        "user": user,
        "password": password,
        "database": database
    }
    if port:
        try:
            cfg["port"] = int(port)
        except ValueError:
            cfg["port"] = port
    return cfg

st.set_page_config(page_title="Pedido Padaria", layout="wide")

def conectar():
    cfg = db_config()
    if not cfg:
        st.error("Config DB ausente no .env")
        return None
    try:
        return mysql.connector.connect(
            **cfg,
            autocommit=False
        )
    except Error as e:
        st.error(f"Erro ao conectar: {e}")
        return None

@st.cache_data
def carregar_fornecedores():
    url = os.getenv("FORNECEDORES_URL")
    if not url:
        st.error("FORNECEDORES_URL ausente no .env")
        return []
    df = pd.read_csv(url, dtype=str)
    primeira_coluna = df.columns[0]
    codigos = df[primeira_coluna].dropna().tolist()
    return [c.strip() for c in codigos if c.strip() != ""]

lista_fornecedores = carregar_fornecedores()

def buscar_produtos(loja):
    conn = conectar()
    if not conn:
        return pd.DataFrame()
    
    try:
        cursor = conn.cursor(dictionary=True)
        
        if not lista_fornecedores:
            st.warning("Lista de fornecedores vazia!")
            return pd.DataFrame()
            
        placeholders = ",".join(["%s"] * len(lista_fornecedores))

        query1 = f"""
        SELECT 
            for_listapre.CODIGOINT,
            cad_mercador.DESCRICAO
        FROM 
            for_listapre
        INNER JOIN cad_mercloja      
            ON for_listapre.CODIGOINT = cad_mercloja.CODIGOINT
        INNER JOIN cad_mercador      
            ON cad_mercloja.CODIGOINT = cad_mercador.CODIGOINT
        WHERE 
            for_listapre.CODIGOFORNEC IN ({placeholders})
            AND cad_mercloja.LOJA = %s
            AND cad_mercloja.ltmix = 'A'
            AND cad_mercador.DESCRICAO LIKE '%cong%'
        GROUP BY 
            for_listapre.CODIGOINT, cad_mercador.DESCRICAO
        ORDER BY cad_mercador.DESCRICAO
        """

        params1 = (*lista_fornecedores, loja)
        cursor.execute(query1, params1)
        result1 = cursor.fetchall()
        
        query2 = """
        SELECT 
            cad_categoriasitens.CODIGOINT,
            cad_mercador.DESCRICAO
        FROM 
            cad_categoriasitens
        INNER JOIN cad_mercador 
            ON cad_categoriasitens.CODIGOINT = cad_mercador.CODIGOINT
        WHERE 
            cad_categoriasitens.CodCategorias = '1935'
        ORDER BY cad_mercador.DESCRICAO
        """
        
        cursor.execute(query2)
        result2 = cursor.fetchall()
        conn.close()
        itens_dict = {}
        
        for row in result1:
            codigo = row["CODIGOINT"]
            if codigo not in itens_dict:
                itens_dict[codigo] = row
        
        for row in result2:
            codigo = row["CODIGOINT"]
            if codigo not in itens_dict:
                itens_dict[codigo] = row

        if itens_dict:
            df = pd.DataFrame(list(itens_dict.values()))
        else:
            df = pd.DataFrame()
            
        return df
        
    except Error as e:
        st.error(f"Erro SQL: {e}")
        print(f"Erro completo: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado: {e}")
        print(f"Erro completo: {e}")
        return pd.DataFrame()


def salvar_pedidos(df, loja):
    conn = conectar()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        inseridos = 0

        for _, row in df.iterrows():
            qnt_str = str(row["Quantidade"]).strip() if pd.notna(row["Quantidade"]) else ""
            
            if qnt_str == "":
                continue
                
            try:
                quantidade = float(qnt_str.replace(",", ".")) if qnt_str else 0.0
            except (ValueError, TypeError):
                continue

            if pd.notna(row["Estoque Alto"]) and row["Estoque Alto"] == True:
                valor_est_alto = -1
            else:
                valor_est_alto = 0

            insert = """
                INSERT INTO app_ped_pad (loja, codigoint, qnt_est, est_alto, data_inclusao)
                VALUES (%s, %s, %s, %s, %s)
            """

            cursor.execute(insert, (
                loja,
                row["CODIGOINT"],
                quantidade,
                valor_est_alto,
                datetime.now()
            ))
            inseridos += 1

        conn.commit()
        conn.close()
        
        if inseridos > 0:
            pass
        else:
            st.warning("⚠️ Nenhum item com quantidade preenchida para salvar.")
            
        return True

    except Error as e:
        conn.rollback()
        st.error(f"Erro ao salvar no banco: {e}")
        print(f"Erro detalhado: {e}")
        return False

col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    st.title("🛒 Pedidos Padaria")
    st.markdown("---")
    
    lojas = [f"{i:03d}" for i in range(1, 27) if i != 20]
    loja = st.selectbox("Selecione a Loja:", lojas, label_visibility="visible")
    
    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 1])
    with col_btn2:
        buscar_clicked = st.button("Buscar Itens", use_container_width=True)
    
    if buscar_clicked:
        df = buscar_produtos(loja)
        
        if df.empty:
            st.warning("Nenhum item encontrado.")
        else:
            df["Quantidade"] = ""
            df["Estoque Alto"] = False
            st.session_state["itens"] = df
            st.session_state["itens_original"] = df.copy()
    
    if "itens" in st.session_state:
        if "itens_original" in st.session_state:
            df_original = st.session_state["itens_original"].copy()
            
            output = BytesIO()
            
            df_original[['CODIGOINT', 'DESCRICAO', 'Quantidade']].to_excel(
                output, 
                index=False, 
                sheet_name='Itens',
                engine='openpyxl'
            )
            
            col_download1, col_download2, col_download3 = st.columns([1, 1, 1])
            with col_download2:
                st.download_button(
                    label="📥 Baixar Tabela",
                    data=output.getvalue(),
                    file_name=f"tabela_padaria_loja_{loja}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
        
        st.subheader("Preencha as Quantidades")
        
        with st.form("form_pedidos"):
            df_editado = st.data_editor(
                st.session_state["itens"],
                key="editor",
                height=750,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "CODIGOINT": st.column_config.Column("Código", width="medium"),
                    "DESCRICAO": st.column_config.Column("Descrição", width="large"),
                    "Quantidade": st.column_config.NumberColumn(
                        "Quantidade", min_value=0, step=0.01, format="%.2f", width="small"
                    ),
                    "Estoque Alto": st.column_config.CheckboxColumn(
                        "Estoque Alto", width="small"
                    ),
                },
                column_order=["CODIGOINT", "DESCRICAO", "Quantidade", "Estoque Alto"]
            )
            
            col_salvar1, col_salvar2, col_salvar3 = st.columns([1, 1, 1])
            with col_salvar2:
                enviar = st.form_submit_button("Salvar Pedido", use_container_width=True)
        
        if enviar:
            st.session_state["itens"] = df_editado
            if "ultimo_salvamento" in st.session_state and (time.time() - st.session_state["ultimo_salvamento"]) < 5:
                st.stop()
            
            st.session_state["ultimo_salvamento"] = time.time()
            if salvar_pedidos(df_editado, loja):
                success_msg = st.success("✔️ Pedido salvo com sucesso!")
                time.sleep(1)
                st.session_state.pop("itens", None)
                st.session_state.pop("itens_original", None)
                st.rerun()
            else:
                st.error("Erro ao salvar.")
