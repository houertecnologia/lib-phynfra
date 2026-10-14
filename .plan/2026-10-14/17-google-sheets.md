# Implementação do Módulo Google Sheets

## User Prompt
> Crie o arquivo phynfra/google/sheets.py contendo a classe Sheets para leitura e ingestão de planilhas Google Sheets: importe json, a classe Fetcher de phynfra.atomic.http e DataFramer de phynfra.apache.spark; no construtor __init__(self, accessToken), valide se accessToken foi informado; implemente o método read(self, sheetId, sheetRange='A1:ZZ', asDataFramer=True, spark=None) que realiza chamada GET via Fetcher à URL f'https://sheets.googleapis.com/v4/spreadsheets/{sheetId}/values/{sheetRange}' com cabeçalho de Authorization Bearer; valide a resposta da API e extraia a matriz 'values', utilizando a primeira linha como cabeçalho de colunas e as linhas subsequentes como registros mapeados em dicionários (preenchendo células faltantes com None); caso asDataFramer seja False, retorne a lista de dicionários resultante; caso asDataFramer seja True, converta a lista de dicionários para DataFrame do Spark (utilizando a sessão de spark informada ou criando via spark.get()) e retorne a instância do DataFramer já carregada e pronta para encadeamento, seguindo rigorosamente a indentação com tabs e nomes em camelCase.

---

## Análise e Requisitos

1. **Imports e Estrutura de `phynfra/google/sheets.py`**:
   - `import json`
   - `from phynfra.atomic.http import Fetcher`
   - `from phynfra.apache.spark import DataFramer, Spark`
   - Tipos do Spark para fallback de schema vazio caso necessário.

2. **Classe `Sheets` e `__init__`**:
   - Assinatura: `def __init__ (self, accessToken):`
   - Validação se `accessToken` foi informado como string não vazia.
   - Armazenar `self.accessToken = accessToken.strip()`.

3. **Método `read`**:
   - Assinatura: `def read (self, sheetId: str, sheetRange: str = 'A1:ZZ', asDataFramer: bool = True, spark = None):`
   - Validação de `sheetId` e `sheetRange`.
   - Chamada HTTP GET via `Fetcher` para o endpoint:
     `f'https://sheets.googleapis.com/v4/spreadsheets/{sheetId.strip()}/values/{sheetRange.strip()}'`
   - Headers contendo `Authorization: Bearer <accessToken>`.
   - Validação da resposta da API (verificação de status e erro).
   - Extração da matriz `values`: primeira linha como cabeçalho e linhas subsequentes mapeadas em dicionários com `None` para células faltantes.
   - Se `asDataFramer is False`: retornar lista de dicionários.
   - Se `asDataFramer is True`: converter a lista de dicionários em Spark DataFrame utilizando a sessão de spark informada ou criando via `Spark.get()`, atribuir a `dataFramer.dataframe` e retornar a instância de `DataFramer` pronta para encadeamento.

4. **Estilo e Diretrizes do Projeto**:
   - Tabs, tamanho 4.
   - Nomenclatura camelCase para variáveis e identificadores.
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Criar o Arquivo `phynfra/google/sheets.py` [done]
- Implementar a classe `Sheets` com construtor e método `read`.

---

## Análise de Segurança
- **Gerenciamento de Autenticação Bearer**: `accessToken` é transmitido exclusivamente nos headers HTTP via TLS/HTTPS, sem ser exposto em URLs de logs ou query strings.
- **Tratamento de Exceções da API**: Respostas com erro da Google Sheets API são interceptadas e relatadas sem vazar dados confidenciais.
