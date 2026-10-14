# Refatoração da Classe Spark para Suporte a Perfis Predefinidos

## User Prompt
> No arquivo phynfra/apache/spark.py, refatore a classe Spark para suportar perfis predefinidos via argumento opcional profile='delta' (perfis: 'vanilla' com master local[*] e 'delta' com master local[*], io.delta.sql.DeltaSparkSessionExtension, org.apache.spark.sql.delta.catalog.DeltaCatalog, KryoSerializer, autoMerge e partitionOverwriteMode=dynamic), garantindo que qualquer chave informada no dicionário configuration sobrescreva o perfil escolhido, aplicando o log level informado e mantendo a compatibilidade do construtor.

---

## Análise e Requisitos

1. **Assinatura do Construtor**:
   - Manter compatibilidade com chamadas existentes: `def __init__ (self, application: str, configuration: dict = None, level: str = 'WARN', profile: str = 'delta') -> None:`
   - Preservar ordem posicional `(application, configuration, level, profile)` para evitar quebras em chamadas legadas.

2. **Perfis Predefinidos (`PROFILES`)**:
   - `'vanilla'`:
     - `'spark.master'`: `'local[*]'`
   - `'delta'`:
     - `'spark.master'`: `'local[*]'`
     - `'spark.sql.extensions'`: `'io.delta.sql.DeltaSparkSessionExtension'`
     - `'spark.sql.catalog.spark_catalog'`: `'org.apache.spark.sql.delta.catalog.DeltaCatalog'`
     - `'spark.serializer'`: `'org.apache.spark.serializer.KryoSerializer'`
     - `'spark.databricks.delta.schema.autoMerge.enabled'`: `'true'`
     - `'spark.sql.sources.partitionOverwriteMode'`: `'dynamic'`

3. **Sobrescrita de Configurações**:
   - As configurações do perfil selecionado são carregadas inicialmente.
   - Qualquer chave passada no dicionário `configuration` sobrescreve os valores do perfil padrão.

4. **Aplicação do Log Level**:
   - Aplicar `level` no SparkContext (`self.spark.sparkContext.setLogLevel(level)`), com verificação de existência do contexto para compatibilidade com Connect/Classic.

5. **Validação de Entrada e Tratamento de Erros**:
   - Validação de `application` (string não nula).
   - Validação de `profile` (deve ser `'vanilla'`, `'delta'` ou None).
   - Validação de `configuration` (deve ser dicionário ou None).
   - Try/catch por operação lógica (construção de SparkSession e aplicação de log level).

---

## Passos do Plano

### Step 1: Implementação dos Perfis e Refatoração de `Spark.__init__` em `phynfra/apache/spark.py` [done]
- Adicionar o dicionário `PROFILES` contendo as configurações de `'vanilla'` e `'delta'`.
- Atualizar a assinatura de `__init__` incluindo `profile: str = 'delta'`.
- Validar parâmetros de entrada (`application`, `profile`, `configuration`).
- Mesclar configurações do perfil com o dicionário `configuration`.
- Instanciar a `SparkSession` aplicando todas as configurações consolidadas.
- Aplicar o nível de log através do `setLogLevel` no contexto do Spark.

---

## Análise de Segurança
- **Injeção de Configurações / Validação**: Entradas são estritamente validadas quanto a tipo e valores permitidos, prevenindo estados inconsistentes ou injeções arbitrárias.
- **Gerenciamento de Segredos**: Nenhuma chave, credencial ou segredo é hardcoded ou exposto.
- **Auditoria e Logs**: A aplicação do `level` de log permite rastreabilidade operacional adequada sem vazamento de dados sensíveis em stdout/stderr.
