# Validação de Conformidade da Arquitetura Medalhão no Método DataFramer.write

## User Prompt
> No arquivo phynfra/apache/spark.py, dentro da classe DataFramer, atualize o método write para incluir uma validação de conformidade da arquitetura medalhão: defina a verificação para as zonas protegidas ['bronze', 'silver', 'gold'] inspecionando o fullpath em minúsculo (verificando ocorrências como '/{zone}/', '/{zone}', '-{zone}/' ou '-{zone}'); caso o caminho pertença a uma dessas três camadas e o fileFormat resolvido for diferente de 'delta', levante um ValueError informando que as camadas bronze, silver e gold exigem obrigatoriamente o formato Delta Lake; garanta que a camada landing e a diamond permaneçam sem essa restrição, mantendo o padrão camelCase e indentação com tabs.

---

## Análise e Requisitos

1. **Zonas Protegidas**:
   - `protectedZones = ['bronze', 'silver', 'gold']`
   - Zonas como `landing` e `diamond` não sofrem essa restrição e aceitam qualquer formato suportado.

2. **Inspeção do `fullpath`**:
   - Converter para minúsculo: `lowerPath = fullpath.lower()`.
   - Verificar ocorrências para cada zona:
     - `f'/{zone}/'`
     - `f'/{zone}'`
     - `f'-{zone}/'`
     - `f'-{zone}'`

3. **Validação de Formato**:
   - Caso `isProtectedZone == True` e `fileFormat.lower() != 'delta'`:
     - Lançar `ValueError` explicitando que as camadas `bronze`, `silver` e `gold` exigem obrigatoriamente o formato Delta Lake.

4. **Estilo e Diretrizes do Projeto**:
   - Nomenclatura camelCase para variáveis e identificadores (`lowerPath`, `protectedZones`, `isProtectedZone`, etc.).
   - Indentação obrigatória com tabs (tamanho 4).
   - Linhas em branco em blocos conforme `gemini.md`.

---

## Passos do Plano

### Step 1: Atualização do Método `DataFramer.write` em `phynfra/apache/spark.py` [done]
- Inserir a validação de conformidade da arquitetura medalhão após a determinação de `fileFormat`.
- Testar a presença de padrões das camadas `bronze`, `silver` e `gold`.
- Levantar `ValueError` se o formato não for `delta`.

---

## Análise de Segurança
- **Governança de Dados / Arquitetura**: A imposição do formato Delta Lake nas camadas centrais do Data Lakehouse assegura integridade transacional ACID, versionamento (Time Travel) e prevenção de dados corrompidos.
- **Validação Antecipada (Fail-Fast)**: Impede que jobs ou pipelines gravem arquivos em formatos não compatíveis em zonas governadas, evitando retrabalho e quebras em consumidores downstream.
