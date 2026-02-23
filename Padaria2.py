import streamlit as st
import mysql.connector
import os
from datetime import datetime, timedelta, time
import pandas as pd
from io import BytesIO
import xlwt

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

def get_db_cfg():
    cfg = db_config()
    if not cfg:
        raise ValueError("Config DB ausente no .env")
    return cfg

def start_of_day(d):
    return datetime.combine(d, time.min)

def start_of_next_day(d):
    return datetime.combine(d + timedelta(days=1), time.min)

def periodo_app_ped_pad():
    hoje = datetime.today().date()
    dias_desde_segunda = (hoje.weekday() - 0) % 7
    ultima_segunda = hoje - timedelta(days=dias_desde_segunda)
    if hoje.weekday() != 0:
        referencia_segunda = ultima_segunda
    else:
        referencia_segunda = hoje
    inicio = referencia_segunda - timedelta(days=7) + timedelta(days=1)
    fim = referencia_segunda
    return inicio, fim

def buscar_fornecedores(codigos_fornecedores):
    try:
        conn = mysql.connector.connect(
            **get_db_cfg(),
            connection_timeout=10
        )
        cursor = conn.cursor(dictionary=True)
        fornecedores_formatados = ", ".join([f"'{codigo}'" for codigo in codigos_fornecedores])
        sql = f"""
        SELECT Codigo, RAZAOSOCIA
        FROM for_forneced
        WHERE Codigo IN ({fornecedores_formatados})
        ORDER BY RAZAOSOCIA
        """
        cursor.execute(sql)
        fornecedores = cursor.fetchall()
        cursor.close()
        conn.close()
        return fornecedores
    except Exception as e:
        st.error(f"Erro buscando fornecedores: {e}")
        return []

def buscar_embalagens(codigos_produtos, codigo_fornecedor=None):
    if not codigos_produtos:
        return {}
    try:
        conn = mysql.connector.connect(
            **get_db_cfg(),
            connection_timeout=10
        )
        cursor = conn.cursor(dictionary=True)
        produtos_formatados = ", ".join([str(codigo) for codigo in codigos_produtos])
        sql = f"""
        SELECT 
            m.CODIGOINT,
            m.codfornprincipal,
            lp.EMBALAGEM
        FROM cad_mercador m
        LEFT JOIN for_listapre lp ON m.CODIGOINT = lp.CODIGOINT 
            AND m.codfornprincipal = lp.CODIGOFORNEC
        WHERE m.CODIGOINT IN ({produtos_formatados})
        """
        cursor.execute(sql)
        resultados = cursor.fetchall()
        embalagens_dict = {}
        for row in resultados:
            codigo = row['CODIGOINT']
            embalagem = row['EMBALAGEM']
            if embalagem is None:
                embalagem_valor = 1
            else:
                try:
                    embalagem_valor = float(embalagem)
                    if embalagem_valor <= 0:
                        embalagem_valor = 1
                except:
                    embalagem_valor = 1
            embalagens_dict[codigo] = embalagem_valor
        cursor.close()
        conn.close()
        for codigo in codigos_produtos:
            if codigo not in embalagens_dict:
                embalagens_dict[codigo] = 1
        return embalagens_dict
    except Exception as e:
        st.error(f"Erro buscando embalagens: {e}")
        return {codigo: 1 for codigo in codigos_produtos}

def buscar_pedidos_pendentes(loja, codigo_fornecedor):
    try:
        conn = mysql.connector.connect(
            **get_db_cfg(),
            connection_timeout=10
        )
        cursor = conn.cursor(dictionary=True)
        hoje = datetime.today().date()
        dias_desde_segunda = (hoje.weekday() - 0) % 7
        ultima_segunda = hoje - timedelta(days=dias_desde_segunda)
        if hoje.weekday() != 0:
            referencia_segunda = ultima_segunda
        else:
            referencia_segunda = hoje
        semana_passada_inicio = referencia_segunda - timedelta(days=7)
        semana_passada_fim = referencia_segunda - timedelta(days=1)
        inicio_dt = start_of_day(semana_passada_inicio)
        fim_dt = start_of_next_day(semana_passada_fim)
        sql = """
        SELECT 
            fi.CODIGOINT,
            SUM(fi.Qtd * fi.EMBALAGEM) AS QTD_PENDENTE
        FROM for_pedidos fp
        INNER JOIN for_itemped fi ON fp.NroPedido = fi.NroPedido
        WHERE fp.Loja = %s
          AND fp.Codigo = %s
          AND fp.DtPedido >= %s
          AND fp.DtPedido < %s
        GROUP BY fi.CODIGOINT
        """
        cursor.execute(sql, (loja, codigo_fornecedor, inicio_dt, fim_dt))
        resultados = cursor.fetchall()
        pedidos_dict = {row['CODIGOINT']: row['QTD_PENDENTE'] for row in resultados}
        cursor.close()
        conn.close()
        return pedidos_dict
    except Exception as e:
        st.error(f"Erro buscando pedidos pendentes: {e}")
        return {}

def buscar_dados_simplificado(loja, venda_inicio_date, venda_fim_date, multiplicador, codigo_fornecedor=None):
    venda_inicio_dt = start_of_day(venda_inicio_date)
    venda_fim_next_dt = start_of_next_day(venda_fim_date)
    ped_pad_inicio, ped_pad_fim = periodo_app_ped_pad()
    inclusao_inicio_dt = start_of_day(ped_pad_inicio)
    inclusao_fim_next_dt = start_of_next_day(ped_pad_fim)
    try:
        conn = mysql.connector.connect(
            **get_db_cfg(),
            connection_timeout=10
        )
    except Exception as e:
        st.error(f"Erro conectando ao banco: {e}")
        return []
    try:
        cursor = conn.cursor(dictionary=True)
        sql = """
        SELECT 
            ap.codigoint AS CODIGOINT,
            m.DESCRICAO,
            ap.est_alto,
            ml.ESTATUAL AS ESTOQUE_VIRTUAL,
            COALESCE(ap.ESTOQUE_INFORMADO, 0) AS ESTOQUE_INFORMADO,
            COALESCE(sc.VENDAS_DIRETAS, 0) AS VENDAS_DIRETAS,
            m.codfornprincipal
        FROM
            (
                SELECT 
                    codigoint, 
                    SUM(qnt_est) AS ESTOQUE_INFORMADO,
                    MAX(est_alto) AS est_alto
                FROM app_ped_pad
                WHERE loja = %s
                  AND data_inclusao >= %s
                  AND data_inclusao < %s
                GROUP BY codigoint
            ) ap
        LEFT JOIN cad_mercador m
            ON m.CODIGOINT = ap.codigoint
        LEFT JOIN cad_mercloja ml
            ON ml.CODIGOINT = ap.codigoint AND ml.loja = %s
        LEFT JOIN
            (
                SELECT 
                    CODIGOINT, 
                    SUM(Quantidade) AS VENDAS_DIRETAS
                FROM sig_captura
                WHERE SiglaLoja = %s
                  AND DtMovimento >= %s
                  AND DtMovimento < %s
                GROUP BY CODIGOINT
            ) sc
            ON sc.CODIGOINT = ap.codigoint
        WHERE 1=1
        """
        params = [
            loja,
            inclusao_inicio_dt,
            inclusao_fim_next_dt,
            loja,
            loja,
            venda_inicio_dt,
            venda_fim_next_dt
        ]
        if codigo_fornecedor:
            sql += " AND m.codfornprincipal = %s"
            params.append(codigo_fornecedor)
        sql += " ORDER BY m.DESCRICAO"
        cursor.execute(sql, tuple(params))
        produtos_base = cursor.fetchall()
        resultado_final = []
        for produto in produtos_base:
            codigo = produto['CODIGOINT']
            cursor2 = conn.cursor(dictionary=True)
            cursor2.execute("""
                SELECT DISTINCT CODIGOINT
                FROM cad_receitas 
                WHERE CODIGOINSUMO = %s
            """, (codigo,))
            receitas = cursor2.fetchall()
            cursor2.close()
            vendas_receitas_total = 0
            for receita in receitas:
                cursor3 = conn.cursor(dictionary=True)
                cursor3.execute("""
                    SELECT 
                        SUM(Quantidade) AS VENDAS_RECEITA,
                        COALESCE(r.QTD, 1) AS QTD
                    FROM sig_captura sc
                    LEFT JOIN cad_receitas r ON r.CODIGOINT = sc.CODIGOINT AND r.CODIGOINSUMO = %s
                    WHERE sc.SiglaLoja = %s
                      AND sc.DtMovimento >= %s
                      AND sc.DtMovimento < %s
                      AND sc.CODIGOINT = %s
                    GROUP BY r.QTD
                """, (codigo, loja, venda_inicio_dt, venda_fim_next_dt, receita['CODIGOINT']))
                venda_receita = cursor3.fetchone()
                cursor3.close()
                if venda_receita and venda_receita['VENDAS_RECEITA']:
                    qtd = venda_receita['QTD'] or 1
                    vendas_receitas_total += venda_receita['VENDAS_RECEITA'] * qtd
            vendas_diretas_com_mult = produto['VENDAS_DIRETAS'] * multiplicador
            vendas_receitas_com_mult = vendas_receitas_total * multiplicador
            total_vendas_com_mult = vendas_diretas_com_mult + vendas_receitas_com_mult
            resultado_final.append({
                'CODIGOINT': produto['CODIGOINT'],
                'DESCRICAO': produto['DESCRICAO'],
                'EST_ALTO': produto['est_alto'],
                'ESTOQUE_VIRTUAL': produto['ESTOQUE_VIRTUAL'] or 0,
                'ESTOQUE_INFORMADO': produto['ESTOQUE_INFORMADO'] or 0,
                'VENDAS_DIRETAS': produto['VENDAS_DIRETAS'] or 0,
                'VENDAS_DIRETAS_COM_MULT': vendas_diretas_com_mult,
                'VENDAS_RECEITAS': vendas_receitas_total,
                'VENDAS_RECEITAS_COM_MULT': vendas_receitas_com_mult,
                'TOTAL_VENDAS_COM_MULT': total_vendas_com_mult,
                'CODFORNPRINCIPAL': produto['codfornprincipal']
            })
        cursor.close()
        conn.close()
        return resultado_final
    except Exception as e:
        try:
            cursor.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass
        st.error(f"Erro executando a query: {e}")
        return []

st.title("📊Sugestão de Compras Padaria")

if 'dados_finais' not in st.session_state:
    st.session_state.dados_finais = None
if 'df_editado' not in st.session_state:
    st.session_state.df_editado = None
if 'edicoes_salvas' not in st.session_state:
    st.session_state.edicoes_salvas = None

CODIGOS_FORNECEDORES = [
    '088680970001',
    '072724430001', 
    '020211550001',
    '027537460001',
    '213480930002'
]

fornecedores = buscar_fornecedores(CODIGOS_FORNECEDORES)

opcoes_fornecedores = [("Todos", None)]
if fornecedores:
    for fornecedor in fornecedores:
        opcoes_fornecedores.append((f"{fornecedor['RAZAOSOCIA']} ({fornecedor['Codigo']})", fornecedor['Codigo']))

with st.container():
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        lojas_validas = [f"{i:03d}" for i in range(1, 27) if i != 20]
        loja_escolhida = st.selectbox("Loja:", lojas_validas, key="loja_select")
    with col2:
        fornecedor_selecionado_texto = st.selectbox(
            "Fornecedor:",
            options=[opcao[0] for opcao in opcoes_fornecedores],
            format_func=lambda x: x,
            key="fornecedor_select"
        )
        codigo_fornecedor_selecionado = None
        for texto, codigo in opcoes_fornecedores:
            if texto == fornecedor_selecionado_texto:
                codigo_fornecedor_selecionado = codigo
                break
    with col3:
        dt_inicio = st.date_input("Data início (vendas):", value=datetime.today().date(), key="dt_inicio")
    with col4:
        dt_fim = st.date_input("Data fim (vendas):", value=datetime.today().date(), key="dt_fim")
    with col5:
        multiplicador = st.number_input(
            "Multiplicador:",
            min_value=0.0,
            max_value=100.0,
            value=2.2,
            step=0.1,
            format="%.2f",
            key="multiplicador"
        )

incluir_pedidos_pendentes = st.checkbox(
    "Incluir pedidos pendentes nos cálculos",
    value=True,
    key="incluir_pedidos"
)

buscar_clicado = st.button(
    "🔍 Buscar dados", 
    type="primary", 
    use_container_width=True,
    key="buscar_dados"
)

def processar_dados():
    ped_pad_inicio, ped_pad_fim = periodo_app_ped_pad()
    st.info(f"📅 **Período do estoque informado:** {ped_pad_inicio.strftime('%d/%m/%Y')} a {ped_pad_fim.strftime('%d/%m/%Y')}")
    with st.spinner("Buscando dados..."):
        resultado = buscar_dados_simplificado(
            loja_escolhida, 
            dt_inicio, 
            dt_fim, 
            multiplicador,
            codigo_fornecedor_selecionado
        )
    if not resultado:
        st.warning("Nenhum item encontrado para os filtros selecionados.")
        st.session_state.dados_finais = None
        st.session_state.df_editado = None
        st.session_state.edicoes_salvas = None
        return None, None
    codigos_produtos = [item['CODIGOINT'] for item in resultado]
    embalagens = buscar_embalagens(codigos_produtos, codigo_fornecedor_selecionado)
    if codigo_fornecedor_selecionado:
        pedidos_pendentes = buscar_pedidos_pendentes(loja_escolhida, codigo_fornecedor_selecionado)
    else:
        pedidos_pendentes = {}
        for codigo_fornecedor in CODIGOS_FORNECEDORES:
            pedidos = buscar_pedidos_pendentes(loja_escolhida, codigo_fornecedor)
            for codigo, qtd in pedidos.items():
                if codigo in pedidos_pendentes:
                    pedidos_pendentes[codigo] += qtd
                else:
                    pedidos_pendentes[codigo] = qtd
    dados_finais = []
    for item in resultado:
        if incluir_pedidos_pendentes:
            pedidos_item = pedidos_pendentes.get(item['CODIGOINT'], 0)
        else:
            pedidos_item = 0
        estoque_total = item['ESTOQUE_INFORMADO'] + pedidos_item
        sugestao_compra = item['TOTAL_VENDAS_COM_MULT'] - estoque_total
        sugestao_compra = max(0, sugestao_compra)
        embalagem = embalagens.get(item['CODIGOINT'], 1)
        if embalagem > 0:
            quantidade_caixas = sugestao_compra / embalagem
        else:
            quantidade_caixas = sugestao_compra
        venda_total = item['TOTAL_VENDAS_COM_MULT']
        excesso_boolean = item.get('EST_ALTO') == -1
        
        dados_finais.append({
            'Código': item['CODIGOINT'],
            'Descrição': item['DESCRICAO'] or f"Código {item['CODIGOINT']}",
            'Excesso?': excesso_boolean,
            'Estoque Virtual': item['ESTOQUE_VIRTUAL'],
            'Estoque Informado': round(item['ESTOQUE_INFORMADO'], 2),
            'Pedidos Pendentes': round(pedidos_item, 2),
            'Venda Total': round(venda_total, 2),
            'Embalagem': round(embalagem, 2),
            'Estoque + Pendentes': round(estoque_total, 2),
            'Sugestão de Compra': round(sugestao_compra, 2),
            'Quantidade de Caixas': round(quantidade_caixas, 2),
            'Fornecedor': item.get('CODFORNPRINCIPAL', '')
        })
    
    df_exibicao = pd.DataFrame(dados_finais)
    return dados_finais, df_exibicao

if buscar_clicado:
    dados_finais, df_exibicao = processar_dados()
    if dados_finais is not None:
        st.session_state.dados_finais = dados_finais
        st.session_state.df_editado = df_exibicao.copy()
        st.session_state.edicoes_salvas = df_exibicao.copy()

if st.session_state.dados_finais is not None:
    dados_finais = st.session_state.dados_finais
    total_itens = len(dados_finais)
    total_excesso = sum(1 for item in dados_finais if item['Excesso?'])
    total_venda = sum(item['Venda Total'] for item in dados_finais)
    total_sugestao = sum(item['Sugestão de Compra'] for item in dados_finais)
    total_caixas = sum(item['Quantidade de Caixas'] for item in dados_finais)
    
    if st.session_state.df_editado is None:
        df_exibicao = pd.DataFrame(st.session_state.dados_finais)
    else:
        df_exibicao = st.session_state.df_editado.copy()
    
    df_exibicao['Código'] = df_exibicao['Código'].astype(str)
    df_exibicao_display = df_exibicao.drop(columns=['Fornecedor'], errors='ignore')
    
    column_config = {
        'Código': st.column_config.TextColumn(width='80px', disabled=True),
        'Descrição': st.column_config.TextColumn(width='280px', disabled=True),
        'Excesso?': st.column_config.CheckboxColumn(
            width='60px',
            help="Marque se o produto está em excesso",
            default=False
        ),
        'Estoque Virtual': st.column_config.NumberColumn(format='%d', width='100px', disabled=True),
        'Estoque Informado': st.column_config.NumberColumn(format='%.2f', width='120px', disabled=True),
        'Pedidos Pendentes': st.column_config.NumberColumn(format='%.2f', width='120px', disabled=True),
        'Venda Total': st.column_config.NumberColumn(format='%.2f', width='120px', disabled=True),
        'Embalagem': st.column_config.NumberColumn(
            format='%.2f',
            width='80px',
            help='Embalagem do produto (da tabela for_listapre)',
            disabled=True
        ),
        'Estoque + Pendentes': st.column_config.NumberColumn(
            format='%.2f',
            width='140px',
            help='Estoque + Pedidos Pendentes',
            disabled=True
        ),
        'Sugestão de Compra': st.column_config.NumberColumn(
            format='%.2f',
            width='140px',
            help='Sugestão de compra em unidades',
            disabled=True
        ),
        'Quantidade de Caixas': st.column_config.NumberColumn(
            format='%.2f',
            width='150px',
            help='Quantidade de caixas a comprar = Sugestão ÷ Embalagem'
        )
    }
    
    editor_key = f"data_editor_{loja_escolhida}_{codigo_fornecedor_selecionado}"
    
    df_editado = st.data_editor(
        df_exibicao_display,
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
        height=500,
        key=editor_key
    )
    
    col_salvar1, col_salvar2, col_salvar3 = st.columns([1, 2, 1])

    with col_salvar2:
        if df_editado is not None:
            st.session_state.edicoes_salvas = df_editado.copy()
            st.session_state.edicoes_salvas['Código'] = df_exibicao['Código']
            st.session_state.edicoes_salvas['Fornecedor'] = df_exibicao['Fornecedor']
            
            def gerar_xls():
                df_export = st.session_state.edicoes_salvas.copy()
                df_export_sem_fornecedor = df_export.drop(columns=['Fornecedor'], errors='ignore')
                
                output = BytesIO()
                workbook = xlwt.Workbook(encoding='utf-8')
                worksheet = workbook.add_sheet('Sugestao')
                
                header_style = xlwt.easyxf('font: bold on; align: wrap on, vert centre, horiz center')
                numeric_style = xlwt.easyxf(num_format_str='0.00')
                integer_style = xlwt.easyxf(num_format_str='0')
                
                headers = list(df_export_sem_fornecedor.columns)
                
                for col_num, header in enumerate(headers):
                    worksheet.write(0, col_num, header, header_style)
                
                row_num_excel = 1
                for idx, row in df_export_sem_fornecedor.iterrows():
                    if row['Excesso?'] == True:
                        continue
                    
                    col_num = 0
                    for col_name in headers:
                        value = row[col_name]
                        
                        if col_name == 'Código':
                            cell_value = str(value)
                            worksheet.write(row_num_excel, col_num, cell_value)
                            col_num += 1
                            continue
                        elif col_name == 'Excesso?':
                            cell_value = ""
                            worksheet.write(row_num_excel, col_num, cell_value)
                            col_num += 1
                            continue
                        elif col_name == 'Estoque Virtual':
                            cell_style = integer_style
                        elif col_name in ['Estoque Informado', 'Pedidos Pendentes', 'Estoque + Pendentes', 
                                         'Venda Total', 'Sugestão de Compra', 'Embalagem', 'Quantidade de Caixas']:
                            cell_style = numeric_style
                        else:
                            cell_style = None
                        
                        if cell_style:
                            worksheet.write(row_num_excel, col_num, value, cell_style)
                        else:
                            worksheet.write(row_num_excel, col_num, value)
                        
                        col_num += 1
                    
                    row_num_excel += 1
                
                column_widths = {
                    0: 10,
                    1: 30,
                    2: 8,
                    3: 12,
                    4: 15,
                    5: 15,
                    6: 12,
                    7: 10,
                    8: 15,
                    9: 15,
                    10: 15,
                }
                
                for col_num, width in column_widths.items():
                    if col_num < len(headers):
                        safe_width = min(width * 256, 3000)
                        worksheet.col(col_num).width = int(safe_width)
                
                workbook.save(output)
                return output.getvalue()
            
            xls_data = gerar_xls()

            st.download_button(
                label="💾 Salvar",
                data=xls_data,
                file_name=f"sugestao_compras_loja_{loja_escolhida}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xls",
                mime="application/vnd.ms-excel",
                use_container_width=True,
            )

st.divider()
with st.expander("ℹ️ Informações sobre os cálculos"):
    st.write("""
    **Fórmulas aplicadas:**
    1. **Venda Total** = (Vendas Diretas + Vendas de Receitas × QTD) × Multiplicador
    2. **Estoque Total** = Estoque Informado + Pedidos Pendentes
    3. **Sugestão de Compra** = Venda Total - Estoque Total
    4. **Quantidade de Caixas** = Sugestão de Compra ÷ Embalagem
    """)
