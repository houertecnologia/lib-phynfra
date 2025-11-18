# Phynfra - Python Houer Infraestructure

## Instruções

Para a criação da estrutura da lib-phynfra, esse setup inicial foi feito.

```bash
DIRETORIO=""
mkdir $DIRETORIO && cd $DIRETORIO
python3 -m virtualenv venv
source venv/bin/activate
pip install poetry
poetry init || touch pyproject.toml
```

## Instalação

Para instalação do phynfra no seu projeto Python, 2 opções

### 1) A partir do código-fonte, repositório clonado em sua máquina:

Use este modo caso esteja enfrentando problemas com a biblioteca em si, para evoluções e proposições:

```bash
pip install -e /path/to/phynfra/clone/folder
```

### 2) Diretamente do Github:

```bash
pip install -e git+https://github.com/houertecnologia/lib-phynfra.git@v3.0#egg=phynfra
```

## Execução

> Nota: é recomendado que você cria um ambiente virtual do Python para a instalação dos seus projetos, exceto em um caso, tipo Docker, em que o python e suas bibliotecas são globais à todo o container, ao escopo. Esta criação pode ser feita com `python3 -m venv venv`.

Após a instalação do phynfra, garanta que os módulos Python sejam iniciados *__init__.py* nas pastas de acordo com a sua estrutura. Para testes mais objetivos sobre o funcionamento do phynfra, crie na raiz do seu projeto um **test.py** e defina uma função dentro dele:

```python
from phynfra.atomic.arguments import RunArguments

def run (**kwargs:RunArguments):
	'''
	Test Function
	'''

	print('First phynfra execution!')
	print('SETTINGS:', kwargs['settings'])

	logger = kwargs['logger']
	logger.info('First phynfra execution from LOGGER')

```

e no terminal, chame a função **run** do phynfra de forma direta:

```bash
python -m phynfra --command run --module test.run
```

e caso queira executar a função **run** com suas próprias variáveis de ambiente, crie um arquivo .env na raiz do projeto e chame desta forma:

```ini
AWS_KEY=teste
AWS_REGION=teste
spark.master=teste
spark.jars=teste
SOME_VARIABLE=teste
```

```bash
python -m phynfra --command run --configuration /path/to/.env --module test.run
```

e atente-se ao segundo "print".
