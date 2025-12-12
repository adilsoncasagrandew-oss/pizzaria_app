import psycopg2
import streamlit as st
import pandas as pd
import re
from datetime import datetime, date
from typing import Optional
import io
from io import BytesIO

# ------------------------------------------------------------------
# ATENÇÃO: coloque aqui sua connection string do Neon (exatamente)
# ------------------------------------------------------------------
DB_URL = "postgresql://neondb_owner:npg_7wr8nOlaUQCt@ep-small-fog-adyaz186-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"


def conectar():
    return psycopg2.connect(DB_URL)


st.set_page_config(page_title="Cadastro - Estoque Pizzaria", layout="wide")

# -------------------------
# UTIL: conexão e criação
# -------------------------

def criar_tabelas():
    conn = conectar()
    cur = conn.cursor()

    # tabela fornecedores
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fornecedores (
        id SERIAL PRIMARY KEY,
        codigo INTEGER UNIQUE,
        nome TEXT NOT NULL,
        cnpj TEXT,
        endereco TEXT,
        numero TEXT,
        cidade TEXT,
        estado TEXT,
        telefone TEXT,
        email TEXT,
        contato TEXT,
        observacoes TEXT,
        criado_em TEXT
    )
    """)

    # tabela insumos (inclui preco_ultima_compra)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS insumos (
        id SERIAL PRIMARY KEY,
        codigo INTEGER UNIQUE,
        nome TEXT NOT NULL,
        marca TEXT,
        embalagem TEXT,
        volume TEXT,
        apresentacao TEXT,
        categoria TEXT,
        armazenamento TEXT,
        unidade_consumo TEXT,
        fator_conversao REAL,
        estoque_minimo REAL,
        validade_padrao INTEGER,
        tolerancia_queixa REAL,
        fornecedor_id INTEGER,
        observacoes TEXT,
        criado_em TEXT,
        preco_ultima_compra REAL,
        FOREIGN KEY (fornecedor_id) REFERENCES fornecedores(id)
    )
    """)

    # tabela movimentos (global)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS estoque_movimentacoes (
        id SERIAL PRIMARY KEY,
        insumo_id INTEGER NOT NULL,
        tipo TEXT NOT NULL,
        quantidade REAL NOT NULL,
        unidade TEXT,
        data_mov TEXT NOT NULL,
        lote TEXT,
        validade TEXT,
        fornecedor_id INTEGER,
        observacao TEXT,
        subestoque_destino TEXT,
        criado_em TEXT,
        FOREIGN KEY (insumo_id) REFERENCES insumos(id),
        FOREIGN KEY (fornecedor_id) REFERENCES fornecedores(id)
    )
    """)

    # tabela lotes (global)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lotes (
        id SERIAL PRIMARY KEY,
        insumo_id INTEGER NOT NULL,
        codigo_lote TEXT,
        quantidade_inicial REAL NOT NULL,
        quantidade_atual REAL NOT NULL,
        validade TEXT,
        fornecedor_id INTEGER,
        criado_em TEXT,
        preco_unitario REAL,
        CONSTRAINT fk_insumo FOREIGN KEY (insumo_id) REFERENCES insumos(id),
        CONSTRAINT fk_fornecedor FOREIGN KEY (fornecedor_id) REFERENCES fornecedores(id)
    )
    """)

    # tabela sublotes (para subestoques)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sublotes (
        id SERIAL PRIMARY KEY,
        subestoque TEXT NOT NULL,
        insumo_id INTEGER NOT NULL,
        codigo_lote TEXT,
        quantidade_inicial REAL NOT NULL,
        quantidade_atual REAL NOT NULL,
        validade TEXT,
        fornecedor_id INTEGER,
        criado_em TEXT,
        preco_unitario REAL,
        origem_lote TEXT,
        CONSTRAINT fk_insumo_sub FOREIGN KEY (insumo_id) REFERENCES insumos(id),
        CONSTRAINT fk_fornecedor_sub FOREIGN KEY (fornecedor_id) REFERENCES fornecedores(id)
    )
    """)

    # tabela movimentacoes de subestoque
    cur.execute("""
    CREATE TABLE IF NOT EXISTS subestoque_movimentacoes (
        id SERIAL PRIMARY KEY,
        subestoque TEXT NOT NULL,
        insumo_id INTEGER NOT NULL,
        tipo TEXT NOT NULL,
        quantidade REAL NOT NULL,
        unidade TEXT,
        data_mov TEXT NOT NULL,
        lote TEXT,
        validade TEXT,
        fornecedor_id INTEGER,
        observacao TEXT,
        origem_lote TEXT,
        criado_em TEXT,
        FOREIGN KEY (insumo_id) REFERENCES insumos(id),
        FOREIGN KEY (fornecedor_id) REFERENCES fornecedores(id)
    )
    """)

    conn.commit()
    cur.close()
    conn.close()


criar_tabelas()

# -------------------------
# SUBESTOQUES (lista fixa)
# -------------------------
SUBESTOQUES = [
    "QUENTE - SUBESTOQUE",
    "SUSHI - SUBESTOQUE",
    "PIZZA SALGADA - SUBESTOQUE",
    "PIZZA DOCE - SUBESTOQUE",
    "SORVETE - SUBESTOQUE",
    "HAMBÚRGER - SUBESTOQUE",
    "COPA - SUBESTOQUE",
    "SALÃO - SUBESTOQUE",
    "LAVAGÊ - SUBESTOQUE"
]

# -------------------------
# GERAÇÃO DE CÓDIGO
# -------------------------
def gerar_codigo_fornecedor():
    conn = conectar()
    cur = conn.cursor()
    cur.execute("SELECT MAX(codigo) FROM fornecedores")
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row or not row[0]:
        return 202508001
    return int(row[0]) + 1

def gerar_codigo_insumo():
    conn = conectar()
    cur = conn.cursor()
    cur.execute("SELECT MAX(codigo) FROM insumos")
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row or not row[0]:
        return 202512001
    return int(row[0]) + 1

# -------------------------
# VALIDAÇÕES
# -------------------------
CNPJ_REGEX = re.compile(r'^\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}$')

def validar_cnpj(cnpj):
    if not cnpj: return True
    return bool(CNPJ_REGEX.match(cnpj.strip()))

# -------------------------
# LISTAS
# -------------------------
EMBALAGENS = ["Pacote","Caixa","Balde","Bombona","Saco","Pote","Garrafa","Lata",
              "Envelope","Bisnaga","Galão","Barrica","Fardo","Bandeja","Vácuo",
              "Caixa térmica","Rolo"]

VOLUMES = ["g","kg","L","mL","unid"]
APRESENTACOES = ["Seco","Molhado","Congelado","Resfriado"]
CATEGORIAS = ["Carnes","Peixes & Frutos do Mar","Frios & Laticínios","Hortifruti",
              "Molhos & Temperos","Massas","Bebidas","Embalagens",
              "Produtos de limpeza","Descartáveis","Congelados","Enlatados",
              "Grãos & Farináceos","Doces & Sobremesas","Óleos & Gorduras","Outros"]
ARMAZENAMENTO = ["Ambiente","Geladeira","Freezer","Câmara fria","Área seca","Área climatizada"]

UF = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
      "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]

# -------------------------
# INSERÇÕES
# -------------------------
def inserir_fornecedor(d):
    conn = conectar()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO fornecedores (
                codigo, nome, cnpj, endereco, numero, cidade, estado,
                telefone, email, contato, observacoes, criado_em
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            d.get("codigo"),
            d.get("nome"),
            d.get("cnpj"),
            d.get("endereco"),
            d.get("numero"),
            d.get("cidade"),
            d.get("estado"),
            d.get("telefone"),
            d.get("email"),
            d.get("contato"),
            d.get("observacoes"),
            d.get("criado_em"),
        ))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def inserir_insumo(d):
    conn = conectar()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO insumos (
                codigo, nome, marca, embalagem, volume, apresentacao,
                categoria, armazenamento, unidade_consumo, fator_conversao,
                estoque_minimo, validade_padrao, tolerancia_queixa,
                fornecedor_id, observacoes, criado_em, preco_ultima_compra
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            d.get("codigo"),
            d.get("nome"),
            d.get("marca"),
            d.get("embalagem"),
            d.get("volume"),
            d.get("apresentacao"),
            d.get("categoria"),
            d.get("armazenamento"),
            d.get("unidade_consumo"),
            float(d.get("fator_conversao") or 0.0),
            float(d.get("estoque_minimo") or 0.0),
            int(d.get("validade_padrao") or 0),
            float(d.get("tolerancia_queixa") or 0.0),
            d.get("fornecedor_id"),
            d.get("observacoes"),
            d.get("criado_em"),
            d.get("preco_ultima_compra"),
        ))
        conn.commit()
    finally:
        cur.close()
        conn.close()

# -------------------------
# Lotes: funções (GLOBAL)
# -------------------------
def criar_lote(insumo_id: int, quantidade: float, validade: Optional[str], fornecedor_id: Optional[int], codigo_lote: Optional[str]=None, preco_unitario: Optional[float]=None):
    conn = conectar()
    cur = conn.cursor()
    if not codigo_lote or str(codigo_lote).strip() == "":
        codigo_lote = f"{insumo_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    criado_em = datetime.now().isoformat()
    try:
        cur.execute("""
            INSERT INTO lotes (insumo_id, codigo_lote, quantidade_inicial, quantidade_atual, validade, fornecedor_id, criado_em, preco_unitario)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (insumo_id, codigo_lote, quantidade, quantidade, validade, fornecedor_id, criado_em, preco_unitario))
        lote_id = cur.fetchone()[0]
        conn.commit()
        return lote_id, codigo_lote
    finally:
        cur.close()
        conn.close()

def atualizar_lote_quantidade(lote_id: int, nova_quantidade: float):
    conn = conectar()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE lotes SET quantidade_atual = %s WHERE id = %s", (nova_quantidade, lote_id))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def atualizar_preco_lote(lote_id: int, preco: Optional[float]):
    conn = conectar()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE lotes SET preco_unitario = %s WHERE id = %s", (preco, lote_id))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def obter_lotes_disponiveis(insumo_id: int):
    conn = conectar()
    try:
        df = pd.read_sql_query("""
            SELECT id, codigo_lote, quantidade_inicial, quantidade_atual, validade, fornecedor_id, criado_em, preco_unitario
            FROM lotes
            WHERE insumo_id = %s AND quantidade_atual > 0
            ORDER BY 
                CASE WHEN validade IS NULL THEN '9999-12-31' ELSE validade END ASC,
                criado_em::timestamp ASC
        """, conn, params=(insumo_id,))
        return df
    finally:
        conn.close()

def total_disponivel_insumo(insumo_id: int) -> float:
    conn = conectar()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(SUM(quantidade_atual),0) FROM lotes WHERE insumo_id = %s", (insumo_id,))
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    finally:
        cur.close()
        conn.close()

# -------------------------
# Lotes: funções (SUBESTOQUE)
# -------------------------
def criar_sublote(subestoque: str, insumo_id: int, quantidade: float, validade: Optional[str], fornecedor_id: Optional[int], origem_lote: Optional[str]=None, codigo_lote: Optional[str]=None, preco_unitario: Optional[float]=None):
    conn = conectar()
    cur = conn.cursor()
    if not codigo_lote or str(codigo_lote).strip() == "":
        short = subestoque.split()[0] if subestoque else "SUB"
        codigo_lote = f"{insumo_id}-{short}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    criado_em = datetime.now().isoformat()
    try:
        cur.execute("""
            INSERT INTO sublotes (subestoque, insumo_id, codigo_lote, quantidade_inicial, quantidade_atual, validade, fornecedor_id, criado_em, preco_unitario, origem_lote)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (subestoque, insumo_id, codigo_lote, quantidade, quantidade, validade, fornecedor_id, criado_em, preco_unitario, origem_lote))
        sublote_id = cur.fetchone()[0]
        conn.commit()
        return sublote_id, codigo_lote
    finally:
        cur.close()
        conn.close()

def atualizar_sublote_quantidade(sublote_id: int, nova_quantidade: float):
    conn = conectar()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE sublotes SET quantidade_atual = %s WHERE id = %s", (nova_quantidade, sublote_id))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def obter_lotes_disponiveis_sub(insumo_id: int, subestoque: str):
    conn = conectar()
    try:
        df = pd.read_sql_query("""
            SELECT id, subestoque, codigo_lote, quantidade_inicial, quantidade_atual, validade, fornecedor_id, criado_em, preco_unitario, origem_lote
            FROM sublotes
            WHERE insumo_id = %s AND subestoque = %s AND quantidade_atual > 0
            ORDER BY 
                CASE WHEN validade IS NULL THEN '9999-12-31' ELSE validade END ASC,
                criado_em::timestamp ASC
        """, conn, params=(insumo_id, subestoque))
        return df
    finally:
        conn.close()

def total_disponivel_sub(insumo_id: int, subestoque: str) -> float:
    conn = conectar()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(SUM(quantidade_atual),0) FROM sublotes WHERE insumo_id = %s AND subestoque = %s", (insumo_id, subestoque))
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    finally:
        cur.close()
        conn.close()

# -------------------------
# Movimentações (GLOBAL)
# -------------------------
def registrar_movimentacao(insumo_id, tipo, quantidade, unidade, lote, validade, fornecedor_id, observacao, subestoque_destino=None):
    conn = conectar()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO estoque_movimentacoes (
                insumo_id, tipo, quantidade, unidade, data_mov,
                lote, validade, fornecedor_id, observacao, subestoque_destino, criado_em
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            insumo_id, tipo, quantidade, unidade, datetime.now().isoformat(),
            lote, validade, fornecedor_id, observacao, subestoque_destino, datetime.now().isoformat()
        ))
        conn.commit()
    finally:
        cur.close()
        conn.close()

# -------------------------
# Movimentações (SUBESTOQUE)
# -------------------------
def registrar_movimentacao_sub(subestoque, insumo_id, tipo, quantidade, unidade, lote, validade, fornecedor_id, observacao, origem_lote=None):
    conn = conectar()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO subestoque_movimentacoes (
                subestoque, insumo_id, tipo, quantidade, unidade, data_mov,
                lote, validade, fornecedor_id, observacao, origem_lote, criado_em
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            subestoque, insumo_id, tipo, quantidade, unidade, datetime.now().isoformat(),
            lote, validade, fornecedor_id, observacao, origem_lote, datetime.now().isoformat()
        ))
        conn.commit()
    finally:
        cur.close()
        conn.close()

# -------------------------
# UI: TABS (mantido layout, unificando subestoques e transferências)
# -------------------------
aba = st.tabs(["Cadastro", "Controle de Estoque", "Dashboard", "Gestão de Custos", "Transferências"])

# =========================================================
# ABA 1 — CADASTRO (Fornecedor / Insumo)
# =========================================================
with aba[0]:
    st.title("Cadastro — Estoque Pizzaria")
    st.write("Escolha o tipo de cadastro e preencha o formulário. (sem alterações no layout)")

    tipo = st.radio("Tipo de cadastro", ("Fornecedor", "Insumo"))

    # ---------- CADASTRO FORNECEDOR ----------
    if tipo == "Fornecedor":
        st.header("Cadastrar Fornecedor")
        col1, col2 = st.columns([2, 1])

        with col1:
            nome = st.text_input("Nome do Fornecedor", max_chars=200)
            cnpj = st.text_input("CNPJ (ex: 00.000.000/0000-00)")
            endereco = st.text_input("Endereço (rua/av)")
            numero = st.text_input("Número")
            cidade = st.text_input("Cidade")
            estado = st.selectbox("Estado (UF)", UF)
        with col2:
            telefone = st.text_input("Telefone / WhatsApp")
            email = st.text_input("E-mail")
            contato = st.text_input("Contato principal")
            observacoes = st.text_area("Observações", height=120, key="obs_fornecedor")

        codigo_preview = gerar_codigo_fornecedor()
        st.info(f"Código do fornecedor: {codigo_preview}")

        if st.button("Salvar Fornecedor"):
            if not nome or nome.strip() == "":
                st.warning("Nome do fornecedor é obrigatório.")
            elif not validar_cnpj(cnpj):
                st.warning("Formato de CNPJ inválido.")
            else:
                dados = {
                    "codigo": codigo_preview,
                    "nome": nome.strip(),
                    "cnpj": cnpj.strip(),
                    "endereco": endereco.strip(),
                    "numero": numero.strip(),
                    "cidade": cidade.strip(),
                    "estado": estado,
                    "telefone": telefone.strip(),
                    "email": email.strip(),
                    "contato": contato.strip(),
                    "observacoes": observacoes.strip(),
                    "criado_em": datetime.now().isoformat()
                }
                try:
                    inserir_fornecedor(dados)
                    st.success(f"Fornecedor '{nome}' cadastrado com sucesso.")
                except psycopg2.IntegrityError as e:
                    st.error(f"Erro ao salvar fornecedor (talvez código duplicado). Detalhe: {e}")

        st.markdown("---")
        st.subheader("Fornecedores cadastrados")
        conn = conectar()
        df_for = pd.read_sql_query("SELECT codigo, nome, cnpj, cidade, estado, telefone, email FROM fornecedores ORDER BY id DESC LIMIT 200", conn)
        conn.close()
        st.dataframe(df_for, width="stretch")

    # ---------- CADASTRO INSUMO ----------
    else:
        st.header("Cadastrar Insumo")
        conn = conectar()
        fornecedores_df = pd.read_sql_query("SELECT id, codigo, nome FROM fornecedores ORDER BY nome", conn)
        conn.close()

        col_a, col_b = st.columns([2, 1])
        with col_a:
            nome_insumo = st.text_input("Nome do Insumo", max_chars=200)
            marca = st.text_input("Marca (opcional)")
            categoria = st.selectbox("Categoria do Insumo", CATEGORIAS)
            embalagem = st.selectbox("Tipo de Embalagem", EMBALAGENS)
            volume = st.selectbox("Volume / Unidade de Compra", VOLUMES)
            apresentacao = st.selectbox("Apresentação Física", APRESENTACOES)
            armazenamento = st.selectbox("Armazenamento Adequado", ARMAZENAMENTO)
        with col_b:
            unidade_consumo = st.selectbox("Unidade de Medida Padrão de Consumo", VOLUMES)
            fator_conversao = st.number_input("Fator de conversão (ex: 5000/200 => 25)", min_value=0.0, value=1.0, format="%.4f")
            estoque_minimo = st.number_input("Estoque mínimo", min_value=0.0, value=0.0)
            validade_padrao = st.number_input("Validade padrão (dias)", min_value=0, value=0)
            tolerancia_queixa = st.number_input("Tolerância de quebra (%)", min_value=0.0, value=0.0)

        st.markdown("**Fornecedor padrão (opcional)**")
        if fornecedores_df.shape[0] == 0:
            st.warning("Nenhum fornecedor cadastrado. Cadastre fornecedores antes para vinculá-los aos insumos.")
            fornecedor_selecionado = None
        else:
            fornecedores_df["label"] = fornecedores_df["codigo"].astype(str) + " - " + fornecedores_df["nome"]
            fornecedor_label = st.selectbox("Fornecedor padrão", options=fornecedores_df["label"].tolist())
            fornecedor_idx = fornecedores_df[fornecedores_df["label"] == fornecedor_label]["id"].values[0]
            fornecedor_selecionado = int(fornecedor_idx)

        observacoes_insumo = st.text_area("Observações do insumo", height=120, key="obs_insumo")

        codigo_preview_insumo = gerar_codigo_insumo()
        st.info(f"Código do insumo: {codigo_preview_insumo}")

        preco_ultima = st.number_input("Preço de compra padrão (opcional) — usado para valorações", min_value=0.0, value=0.0, step=0.01, format="%.2f")

        if st.button("Salvar Insumo"):
            if not nome_insumo or nome_insumo.strip() == "":
                st.warning("Nome do insumo é obrigatório.")
            else:
                dados = {
                    "codigo": codigo_preview_insumo,
                    "nome": nome_insumo.strip(),
                    "marca": marca.strip(),
                    "embalagem": embalagem,
                    "volume": volume,
                    "apresentacao": apresentacao,
                    "categoria": categoria,
                    "armazenamento": armazenamento,
                    "unidade_consumo": unidade_consumo,
                    "fator_conversao": float(fator_conversao),
                    "estoque_minimo": float(estoque_minimo),
                    "validade_padrao": int(validade_padrao),
                    "tolerancia_queixa": float(tolerancia_queixa),
                    "fornecedor_id": fornecedor_selecionado,
                    "observacoes": observacoes_insumo.strip(),
                    "criado_em": datetime.now().isoformat(),
                    "preco_ultima_compra": float(preco_ultima) if preco_ultima and preco_ultima>0 else None
                }
                try:
                    inserir_insumo(dados)
                    st.success(f"Insumo '{nome_insumo}' cadastrado com código {codigo_preview_insumo}.")
                except psycopg2.IntegrityError as e:
                    st.error(f"Erro ao salvar insumo (talvez código duplicado). Detalhe: {e}")

        st.markdown("---")
        st.subheader("Insumos cadastrados (resumo)")
        conn = conectar()
        df_ins = pd.read_sql_query("""
            SELECT i.codigo AS codigo_insumo, i.nome, i.marca, i.categoria, i.embalagem, i.volume,
                   f.codigo AS codigo_fornecedor, f.nome AS fornecedor_nome, i.preco_ultima_compra
            FROM insumos i
            LEFT JOIN fornecedores f ON i.fornecedor_id = f.id
            ORDER BY i.id DESC
            LIMIT 300
        """, conn)
        conn.close()
        st.dataframe(df_ins, width="stretch")


# =========================================================
# ABA 2 — CONTROLE DE ESTOQUE (com lotes + FIFO + Transferência)
# =========================================================
with aba[1]:

    st.title("📦 Controle de Estoque")

    # -----------------------------
    # Formulário de movimentação
    # -----------------------------
    st.subheader("Registrar Movimentação")

    insumos_df = pd.read_sql_query("SELECT id, nome, unidade_consumo FROM insumos ORDER BY nome", conectar())
    fornecedores_df = pd.read_sql_query("SELECT id, nome FROM fornecedores ORDER BY nome", conectar())

    if insumos_df.empty:
        st.warning("Nenhum insumo cadastrado.")
    else:
        insumo_nome = st.selectbox("Insumo", insumos_df["nome"].tolist())
        insumo_id = int(insumos_df.loc[insumos_df["nome"] == insumo_nome, "id"].values[0])
        unidade_padrao = insumos_df.loc[insumos_df["nome"] == insumo_nome, "unidade_consumo"].values[0]

        tipo = st.radio("Tipo", ["Entrada", "Saída", "Transferência"], horizontal=True)
        quantidade = st.number_input("Quantidade", min_value=0.01, step=0.01)
        unidade = st.selectbox("Unidade", VOLUMES, index=VOLUMES.index(unidade_padrao) if unidade_padrao in VOLUMES else 0)

        lote_input = st.text_input("Lote (opcional) — se em branco será gerado automaticamente")

        validade_input = st.date_input("Validade (opcional)", value=None)
        validade = str(validade_input) if validade_input else None

        preco_lote_input = st.number_input("Preço unitário do lote (opcional) — deixe em 0 para não informar", min_value=0.0, value=0.0, step=0.01, format="%.2f")

        fornecedor_id = None
        if not fornecedores_df.empty:
            fornecedor_nome = st.selectbox("Fornecedor (opcional)", ["Nenhum"] + fornecedores_df["nome"].tolist())
            if fornecedor_nome != "Nenhum":
                fornecedor_id = int(fornecedores_df.loc[fornecedores_df["nome"] == fornecedor_nome, "id"].values[0])

        subestoque_destino = None
        if tipo == "Transferência":
            subestoque_destino = st.selectbox("Destino (Subestoque)", options=SUBESTOQUES)

        obs = st.text_area("Observações", height=80, key="obs_mov")

        if st.button("Salvar Movimentação"):
            tipo_bd = "entrada" if tipo == "Entrada" else ("saida" if tipo == "Saída" else "transferencia")

            # ENTRADA
            if tipo_bd == "entrada":
                preco_unitario_val = float(preco_lote_input) if preco_lote_input and preco_lote_input>0 else None
                lote_id, codigo_lote = criar_lote(insumo_id, float(quantidade), validade, fornecedor_id, lote_input, preco_unitario_val)
                registrar_movimentacao(insumo_id, tipo_bd, float(quantidade), unidade, codigo_lote, validade, fornecedor_id, obs, subestoque_destino=None)
                st.success(f"Entrada registrada e lote criado: {codigo_lote} (qtd {quantidade} {unidade})")

            # SAÍDA
            elif tipo_bd == "saida":
                total_disponivel = total_disponivel_insumo(insumo_id)
                if total_disponivel <= 0:
                    st.error("Sem estoque disponível para esse insumo.")
                elif float(quantidade) > total_disponivel:
                    st.error(f"Quantidade solicitada ({quantidade}) maior que total disponível ({total_disponivel}). Ajuste a quantidade ou registre novas entradas.")
                else:
                    quantidade_a_consumir = float(quantidade)
                    lotes_df = obter_lotes_disponiveis(insumo_id)
                    for idx, lote_row in lotes_df.iterrows():
                        if quantidade_a_consumir <= 0:
                            break
                        lote_id = int(lote_row["id"])
                        codigo_lote = lote_row["codigo_lote"]
                        disponivel_no_lote = float(lote_row["quantidade_atual"])
                        consumir = min(disponivel_no_lote, quantidade_a_consumir)
                        nova_qtd = round(disponivel_no_lote - consumir, 6)
                        atualizar_lote_quantidade(lote_id, nova_qtd)
                        registrar_movimentacao(insumo_id, tipo_bd, float(consumir), unidade, codigo_lote, lote_row["validade"], fornecedor_id, obs)
                        quantidade_a_consumir = round(quantidade_a_consumir - consumir, 6)
                    st.success(f"Saída registrada ({quantidade} {unidade}) — consumido por FIFO em lotes.")

            # TRANSFERÊNCIA
            else:  # transferencia
                if not subestoque_destino:
                    st.error("Selecione o subestoque de destino para realizar a transferência.")
                else:
                    total_disponivel = total_disponivel_insumo(insumo_id)
                    if total_disponivel <= 0:
                        st.error("Sem estoque disponível para esse insumo.")
                    elif float(quantidade) > total_disponivel:
                        st.error(f"Quantidade solicitada ({quantidade}) maior que total disponível ({total_disponivel}).")
                    else:
                        quantidade_a_transferir = float(quantidade)
                        lotes_df = obter_lotes_disponiveis(insumo_id)
                        for idx, lote_row in lotes_df.iterrows():
                            if quantidade_a_transferir <= 0:
                                break
                            lote_id = int(lote_row["id"])
                            codigo_lote_origem = lote_row["codigo_lote"]
                            disponivel_no_lote = float(lote_row["quantidade_atual"])
                            consumir = min(disponivel_no_lote, quantidade_a_transferir)
                            nova_qtd = round(disponivel_no_lote - consumir, 6)
                            atualizar_lote_quantidade(lote_id, nova_qtd)
                            preco_origem = lote_row.get("preco_unitario", None)
                            fornecedor_origem = lote_row.get("fornecedor_id", None)
                            # criar sublote no destino
                            sublote_id, codigo_lote_destino = criar_sublote(
                                subestoque_destino, insumo_id, float(consumir), lote_row["validade"], fornecedor_origem,
                                origem_lote=codigo_lote_origem, codigo_lote=None, preco_unitario=preco_origem
                            )
                            # registrar movimentos
                            registrar_movimentacao(insumo_id, "transferencia", float(consumir), unidade, codigo_lote_origem, lote_row["validade"], fornecedor_origem, obs, subestoque_destino=subestoque_destino)
                            registrar_movimentacao_sub(subestoque_destino, insumo_id, "entrada_por_transferencia", float(consumir), unidade, codigo_lote_destino, lote_row["validade"], fornecedor_origem, obs, origem_lote=codigo_lote_origem)
                            quantidade_a_transferir = round(quantidade_a_transferir - consumir, 6)
                        st.success(f"Transferência de {quantidade} {unidade} para '{subestoque_destino}' registrada com sucesso.")

    st.markdown("---")

    # -----------------------------
    # ESTOQUE ATUAL (agregado por insumo)
    # -----------------------------
    st.subheader("📊 Estoque Atual (agregado por insumo)")

    conn = conectar()
    df_estoque = pd.read_sql_query("""
        SELECT 
            i.id AS insumo_id,
            i.nome AS insumo,
            i.unidade_consumo,
            i.estoque_minimo,
            COALESCE((SELECT SUM(l.quantidade_atual) FROM lotes l WHERE l.insumo_id = i.id), 0) AS estoque_atual
        FROM insumos i
        ORDER BY i.nome
    """, conn)
    conn.close()

    df_estoque["⚠️ Alerta"] = df_estoque.apply(
        lambda row: "🔴 Abaixo do mínimo" if row["estoque_atual"] < (row["estoque_minimo"] or 0) else "",
        axis=1
    )

    st.dataframe(df_estoque, width="stretch")

    st.markdown("---")

    # -----------------------------
    # Lotes ativos (detalhado)
    # -----------------------------
    st.subheader("Lotes ativos (por insumo)")

    conn = conectar()
    df_lotes = pd.read_sql_query("""
        SELECT
            l.id,
            i.nome AS insumo,
            l.codigo_lote,
            l.quantidade_inicial,
            l.quantidade_atual,
            l.validade,
            f.nome AS fornecedor,
            l.criado_em,
            l.preco_unitario
        FROM lotes l
        LEFT JOIN insumos i ON l.insumo_id = i.id
        LEFT JOIN fornecedores f ON l.fornecedor_id = f.id
        ORDER BY l.validade IS NULL, l.validade, l.criado_em::timestamp
    """, conn)
    conn.close()

    if df_lotes.empty:
        st.info("Nenhum lote registrado ainda.")
    else:
        df_lotes["Status"] = df_lotes.apply(
            lambda r: (
                "❗ Vencido" if (r["validade"] and pd.to_datetime(r["validade"]).date() < date.today())
                else ("🔴 Esgotado" if float(r["quantidade_atual"]) <= 0 else "")
            ), axis=1
        )
        st.dataframe(df_lotes, width="stretch")

    st.markdown("---")

    # -----------------------------
    # HISTÓRICO COMPLETO (mantém)
    # -----------------------------
    st.subheader("Histórico de Movimentações")

    conn = conectar()
    df_hist = pd.read_sql_query("""
        SELECT 
            m.data_mov,
            i.nome AS insumo,
            m.tipo,
            m.quantidade,
            m.unidade,
            m.lote,
            m.validade,
            f.nome AS fornecedor,
            m.subestoque_destino,
            m.observacao
        FROM estoque_movimentacoes m
        LEFT JOIN insumos i ON m.insumo_id = i.id
        LEFT JOIN fornecedores f ON m.fornecedor_id = f.id
        ORDER BY m.id DESC
        LIMIT 500
    """, conn)
    conn.close()

    st.dataframe(df_hist, width="stretch")


# =========================================================
# ABA 3 — DASHBOARD (Entradas x Saídas)
# =========================================================
with aba[2]:
    st.title("📈 Dashboard — Histórico de Entradas e Saídas")
    st.write("Escolha um insumo para visualizar o histórico de entradas (gráfico 1) e saídas (gráfico 2).")

    conn = conectar()
    insumos_all = pd.read_sql_query("SELECT id, nome, unidade_consumo FROM insumos ORDER BY nome", conn)
    conn.close()

    if insumos_all.empty:
        st.info("Nenhum insumo cadastrado. Cadastre insumos primeiro.")
    else:
        insumo_sel_name = st.selectbox("Selecione o Insumo", options=insumos_all["nome"].tolist())
        insumo_sel_id = int(insumos_all.loc[insumos_all["nome"] == insumo_sel_name, "id"].values[0])
        unidade_padrao = insumos_all.loc[insumos_all["nome"] == insumo_sel_name, "unidade_consumo"].values[0]

        col1, col2 = st.columns([1, 2])
        with col1:
            inicio = st.date_input("Data Início", value=(date.today().replace(day=1)))
        with col2:
            fim = st.date_input("Data Fim", value=date.today())

        conn = conectar()
        df_mov = pd.read_sql_query("""
            SELECT data_mov, tipo, quantidade
            FROM estoque_movimentacoes
            WHERE insumo_id = %s
            ORDER BY criado_em::timestamp
        """, conn, params=(insumo_sel_id,))
        conn.close()

        if df_mov.empty:
            st.info("Nenhuma movimentação registrada para este insumo.")
        else:
            df_mov["data_mov"] = pd.to_datetime(df_mov["data_mov"], errors="coerce")
            df_mov = df_mov.dropna(subset=["data_mov"])
            df_mov["data"] = df_mov["data_mov"].dt.date
            df_mov = df_mov[(df_mov["data"] >= inicio) & (df_mov["data"] <= fim)]

            if df_mov.empty:
                st.info("Nenhuma movimentação no período selecionado.")
            else:
                # Entradas
                df_entradas = df_mov[df_mov["tipo"] == "entrada"].groupby("data").agg({"quantidade": "sum"}).reset_index()

                # Saídas agora incluem 'saida' e 'transferencia'
                df_saidas = df_mov[df_mov["tipo"].isin(["saida", "transferencia"])].groupby("data").agg({"quantidade": "sum"}).reset_index()

                df_entradas.index = pd.to_datetime(df_entradas["data"])
                df_saidas.index = pd.to_datetime(df_saidas["data"])

                idx = pd.date_range(start=inicio, end=fim)
                entradas_ts = df_entradas.reindex(idx, fill_value=0)["quantidade"]
                saidas_ts = df_saidas.reindex(idx, fill_value=0)["quantidade"]

                st.subheader("Gráfico 1 — Entradas")
                st.line_chart(entradas_ts)

                st.subheader("Gráfico 2 — Saídas")
                st.line_chart(saidas_ts)

                total_entrada = float(entradas_ts.sum())
                total_saida = float(saidas_ts.sum())
                estoque_atual = total_disponivel_insumo(insumo_sel_id)

                st.markdown("---")
                st.write(f"**Resumo ({inicio.isoformat()} → {fim.isoformat()})**")
                st.write(f"- Total Entrado: **{total_entrada} {unidade_padrao}**")
                st.write(f"- Total Saído: **{total_saida} {unidade_padrao}**")
                st.write(f"- Estoque disponível (todos os lotes): **{estoque_atual} {unidade_padrao}**")

                st.markdown("---")
                st.info("Os gráficos mostram a soma diária de quantidades por tipo (entrada/saída).")


# =========================================================
# ABA 4 — GESTÃO DE CUSTOS
# =========================================================
with aba[3]:
    st.title("💰 Gestão de Custos")
    st.write("Valorização do estoque, CMC (custo médio) e preço por lote. Atualize preços para calcular o valor total do estoque.")

    conn = conectar()
    insumos_cost = pd.read_sql_query("SELECT id, nome, unidade_consumo, preco_ultima_compra FROM insumos ORDER BY nome", conn)
    conn.close()

    if insumos_cost.empty:
        st.info("Nenhum insumo cadastrado.")
    else:
        insumo_sel = st.selectbox("Selecione o insumo", options=insumos_cost["nome"].tolist())
        insumo_id = int(insumos_cost.loc[insumos_cost["nome"] == insumo_sel, "id"].values[0])
        unidade_padrao = insumos_cost.loc[insumos_cost["nome"] == insumo_sel, "unidade_consumo"].values[0]
        preco_padrao_insumo = insumos_cost.loc[insumos_cost["nome"] == insumo_sel, "preco_ultima_compra"].values[0]

        st.subheader("Preço padrão do insumo")
        colp1, colp2 = st.columns([3,1])
        with colp1:
            preco_atual = st.number_input("Preço unitário padrão do insumo (preço_ultima_compra)", min_value=0.0, value=float(preco_padrao_insumo) if preco_padrao_insumo and preco_padrao_insumo>0 else 0.0, step=0.01, format="%.2f")
        with colp2:
            if st.button("Atualizar Preço do Insumo"):
                conn = conectar()
                cur = conn.cursor()
                try:
                    cur.execute("UPDATE insumos SET preco_ultima_compra = %s WHERE id = %s", (float(preco_atual), insumo_id))
                    conn.commit()
                    st.success("Preço atualizado.")
                finally:
                    cur.close()
                    conn.close()

        st.markdown("---")
        st.subheader("Lotes e preços (edite preços por lote se necessário)")

        conn = conectar()
        df_lotes_cost = pd.read_sql_query("""
            SELECT id, codigo_lote, quantidade_inicial, quantidade_atual, validade, fornecedor_id, criado_em, preco_unitario
            FROM lotes
            WHERE insumo_id = %s
            ORDER BY validade IS NULL, validade, criado_em::timestamp
        """, conn, params=(insumo_id,))
        conn.close()

        if df_lotes_cost.empty:
            st.info("Nenhum lote registrado para este insumo.")
        else:
            df_display = df_lotes_cost.copy()
            df_display["preco_unitario"] = df_display["preco_unitario"].fillna(0.0)
            updated_any = False
            for i, row in df_display.iterrows():
                st.write(f"Lote: **{row['codigo_lote']}** — validade: {row['validade']} — disponível: {row['quantidade_atual']}")
                col1, col2 = st.columns([2,1])
                with col1:
                    p = st.number_input(f"Preço unitário — lote {row['id']}", min_value=0.0, value=float(row['preco_unitario']) if row['preco_unitario'] else 0.0, step=0.01, format="%.2f", key=f"preco_lote_{row['id']}")
                with col2:
                    if st.button(f"Salvar preço lote {row['id']}", key=f"btn_save_preco_{row['id']}"):
                        atualizar_preco_lote(int(row['id']), float(p))
                        st.success(f"Preço do lote {row['codigo_lote']} atualizado para {p}")
                        updated_any = True
            if updated_any:
                conn = conectar()
                df_lotes_cost = pd.read_sql_query("""
                    SELECT id, codigo_lote, quantidade_inicial, quantidade_atual, validade, fornecedor_id, criado_em, preco_unitario
                    FROM lotes
                    WHERE insumo_id = %s
                    ORDER BY validade IS NULL, validade, criado_em::timestamp
                """, conn, params=(insumo_id,))
                conn.close()

            def preco_para_lote(row):
                if row["preco_unitario"] and row["preco_unitario"]>0:
                    return float(row["preco_unitario"])
                if preco_padrao_insumo and preco_padrao_insumo>0:
                    return float(preco_padrao_insumo)
                return 0.0

            df_lotes_cost["preco_usado"] = df_lotes_cost.apply(preco_para_lote, axis=1)
            df_lotes_cost["valor_em_estoque"] = df_lotes_cost["quantidade_atual"].astype(float) * df_lotes_cost["preco_usado"].astype(float)

            st.markdown("**Resumo por lote**")
            st.dataframe(df_lotes_cost[["codigo_lote","quantidade_atual","preco_usado","valor_em_estoque","validade"]], width="stretch")

            total_q = df_lotes_cost["quantidade_atual"].astype(float).sum()
            total_valor = df_lotes_cost["valor_em_estoque"].astype(float).sum()
            cmc = (total_valor / total_q) if total_q>0 else 0.0

            st.markdown("---")
            st.subheader("Resumo financeiro")
            st.write(f"- Estoque disponível (soma lotes): **{total_q} {unidade_padrao}**")
            st.write(f"- Valor total em estoque (considerando preços por lote ou preço padrão): **R$ {total_valor:.2f}**")
            st.write(f"- CMC (Custo Médio Contábil) baseado nos lotes atuais: **R$ {cmc:.4f} por {unidade_padrao}**")

            csv_buffer = io.StringIO()
            df_lotes_cost.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue().encode('utf-8')
            st.download_button("Exportar dados de lotes/valores (CSV)", data=csv_bytes, file_name=f"custos_insumo_{insumo_sel.replace(' ','_')}.csv", mime="text/csv")

    st.markdown("---")
    st.subheader("Relatório geral — Valor total do estoque por insumo")

    conn = conectar()
    df_all = pd.read_sql_query("""
        SELECT i.id AS insumo_id, i.nome, i.unidade_consumo, i.preco_ultima_compra,
               COALESCE((SELECT SUM(l.quantidade_atual * COALESCE(l.preco_unitario, i.preco_ultima_compra)) FROM lotes l WHERE l.insumo_id = i.id), 0) AS valor_em_estoque,
               COALESCE((SELECT SUM(l.quantidade_atual) FROM lotes l WHERE l.insumo_id = i.id), 0) AS estoque_atual
        FROM insumos i
        ORDER BY i.nome
    """, conn)
    conn.close()

    if df_all.empty:
        st.info("Nenhum insumo com lotes/preços.")
    else:
        df_all["valor_em_estoque"] = df_all["valor_em_estoque"].astype(float)
        df_all["estoque_atual"] = df_all["estoque_atual"].astype(float)
        df_all["cmc"] = df_all.apply(lambda r: (r["valor_em_estoque"] / r["estoque_atual"]) if r["estoque_atual"]>0 else 0.0, axis=1)
        st.dataframe(df_all[["nome","estoque_atual","cmc","valor_em_estoque"]], width="stretch")

        total_geral = df_all["valor_em_estoque"].sum()
        st.markdown(f"**Valor total do estoque (todos os insumos): R$ {total_geral:.2f}**")

        csv_buffer_all = io.StringIO()
        df_all.to_csv(csv_buffer_all, index=False)
        st.download_button("Exportar relatório geral (CSV)", data=csv_buffer_all.getvalue().encode('utf-8'), file_name="relatorio_valor_estoque.csv", mime="text/csv")

    st.markdown("---")
    st.info("Observações: \n- Preencha preços por lote sempre que possível para ter avaliação correta.\n- Se um lote não tiver preço preenchido, usamos o preço_ultima_compra do insumo (se existir) para valorizar o lote.\n- O CMC aqui é calculado sobre os lotes atuais (quantidade_atual × preço). Se quiser que o CMC seja calculado por histórico de entradas, eu ajusto.")


# =========================================================
# ABA 5 — TRANSFERÊNCIAS (unificada: visão por subestoque + filtro de data + PDF)
# =========================================================
with aba[4]:
    st.title("🔁 Transferências — Subestoques")
    st.write("Selecione o subestoque para visualizar a última movimentação de transferência do dia e gerar impressão (PDF).")

    subestoque_sel = st.selectbox("Selecione o Subestoque", options=SUBESTOQUES, key="select_subestoque_transfer")

    # Filtro de data (movimentação diária)
    data_filtro = st.date_input("Data (filtrar movimentações deste dia)", value=date.today(), key="filtro_data_transfer")

    conn = conectar()
    # buscamos apenas movimentações de subestoque que tenham origem_lote (transferências) para o dia selecionado
    df_mov = pd.read_sql_query("""
        SELECT m.id, m.data_mov, i.nome AS insumo, m.quantidade, m.unidade, m.lote, m.origem_lote
        FROM subestoque_movimentacoes m
        LEFT JOIN insumos i ON m.insumo_id = i.id
        WHERE m.subestoque = %s
          AND date(m.data_mov) = date(%s)
          AND m.origem_lote IS NOT NULL
        ORDER BY m.id DESC
        LIMIT 1
    """, conn, params=(subestoque_sel, str(data_filtro)))
    conn.close()

    st.subheader(f"Última movimentação em {data_filtro.isoformat()} — {subestoque_sel}")

    if df_mov.empty:
        st.info("Nenhuma movimentação de transferência encontrada para esta data.")
    else:
        # mostramos apenas insumo e quantidade conforme solicitado
        df_display = df_mov[["insumo", "quantidade"]].copy()
        st.table(df_display)

        # Botão para gerar PDF da movimentação
        if st.button("Gerar PDF da última movimentação"):
            try:
                from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
                from reportlab.lib.styles import getSampleStyleSheet
                from reportlab.lib.pagesizes import A4

                buffer = BytesIO()
                doc = SimpleDocTemplate(buffer, pagesize=A4)
                styles = getSampleStyleSheet()
                story = []

                story.append(Paragraph(f"Transferência — {subestoque_sel}", styles['Title']))
                story.append(Spacer(1, 12))
                story.append(Paragraph(f"Data: {data_filtro.isoformat()}", styles['Normal']))
                story.append(Spacer(1, 12))
                story.append(Paragraph(f"Insumo: {df_mov.iloc[0]['insumo']}", styles['Normal']))
                story.append(Paragraph(f"Quantidade: {df_mov.iloc[0]['quantidade']} {df_mov.iloc[0]['unidade']}", styles['Normal']))
                story.append(Paragraph(f"Lote origem: {df_mov.iloc[0]['origem_lote']}", styles['Normal']))

                doc.build(story)
                buffer.seek(0)

                st.download_button("Baixar PDF", data=buffer.getvalue(), file_name=f"transferencia_{subestoque_sel.replace(' ','_')}_{data_filtro.isoformat()}.pdf", mime="application/pdf")
            except Exception as e:
                # fallback: gerar um arquivo texto se reportlab não estiver disponível
                txt = f"Transferência - {subestoque_sel}\nData: {data_filtro.isoformat()}\nInsumo: {df_mov.iloc[0]['insumo']}\nQuantidade: {df_mov.iloc[0]['quantidade']} {df_mov.iloc[0]['unidade']}\nLote origem: {df_mov.iloc[0]['origem_lote']}\n"
                st.download_button("Baixar (TXT) — reportlab ausente", data=txt, file_name=f"transferencia_{subestoque_sel.replace(' ','_')}_{data_filtro.isoformat()}.txt", mime="text/plain")

    st.markdown("---")
    st.subheader("Histórico resumido (últimas 50 transferências para o subestoque)")
    conn = conectar()
    df_hist_short = pd.read_sql_query("""
        SELECT m.data_mov, i.nome AS insumo, m.quantidade, m.unidade, m.lote, m.origem_lote
        FROM subestoque_movimentacoes m
        LEFT JOIN insumos i ON m.insumo_id = i.id
        WHERE m.subestoque = %s AND m.origem_lote IS NOT NULL
        ORDER BY m.id DESC
        LIMIT 50
    """, conn, params=(subestoque_sel,))
    conn.close()

    if df_hist_short.empty:
        st.info("Nenhuma transferência registrada para este subestoque.")
    else:
        st.dataframe(df_hist_short[["data_mov","insumo","quantidade","unidade","origem_lote"]], width="stretch")

# EOF
