# MapReduce - Correlação da temperatura

Calcula a correlação (Pearson) entre a **coluna 9 (temp)** do arquivo `forestfireinput.csv`
e todas as outras colunas, usando MapReduce em Java no Hadoop.

Colunas do arquivo: `X, Y, month, day, FFMC, DMC, DC, ISI, temp, RH, wind, rain, area`
(`month` e `day` são texto, então o Map converte para número: jan = 1 ... dec = 12 e mon = 1 ... sun = 7).

## Como funciona

O HDFS divide o arquivo em blocos e cada slave roda o Map só no bloco que tem.
A correlação de um bloco é diferente da correlação do arquivo inteiro. Por isso a
correlação **não** é calculada nos slaves:

- **Map**: lê todas as linhas do bloco e confere se cada linha está íntegra (13 colunas,
  valores numéricos, mês/dia válidos, sem valores impossíveis). Se tiver algum problema,
  a linha inteira é descartada. Com as linhas válidas ele vai somando `n, Σx, Σy, Σx², Σy², Σxy`
  (x = temp, y = outra coluna). Só no `cleanup()`, depois de ler o bloco todo, ele envia as somas.
- **Reduce**: recebe as somas de todos os slaves, junta tudo e calcula

```
r = (n·Σxy − Σx·Σy) / √[(n·Σx² − (Σx)²) · (n·Σy² − (Σy)²)]
```

Os contadores `LIDAS`, `VALIDAS` e `INVALIDAS` aparecem no final da execução e mostram
se todas as linhas foram lidas.

## Preparar o ambiente (Docker)

Usei o Docker Desktop com o cluster Hadoop do projeto
[big-data-europe/docker-hadoop](https://github.com/big-data-europe/docker-hadoop)
(Hadoop 3.2.1 + Java 8, com namenode, datanode, resourcemanager, nodemanager e historyserver).

1. Instalar o WSL 2 (PowerShell como administrador) e reiniciar o PC:
   ```
   wsl --install
   ```
2. Instalar o [Docker Desktop](https://www.docker.com/products/docker-desktop/) e abrir ele.
3. Baixar e subir o cluster Hadoop (na primeira vez demora para baixar as imagens):
   ```
   git clone --config core.autocrlf=false https://github.com/big-data-europe/docker-hadoop.git
   cd docker-hadoop
   docker compose up -d
   docker ps
   ```
   O `docker ps` tem que mostrar os containers `namenode`, `datanode`, `resourcemanager`,
   `nodemanager` e `historyserver`. A interface do HDFS fica em http://localhost:9870.

Para desligar depois: `docker compose stop` (na pasta `docker-hadoop`).

## Como rodar

Com os containers rodando, abra o terminal na pasta deste projeto.

**1. Copiar os arquivos para o container do namenode**

```
docker cp CorrelacaoTemperatura.java namenode:/tmp/
docker cp forestfireinput.csv namenode:/tmp/
```

**2. Entrar no container**

```
docker exec -it namenode bash
cd /tmp
```

**3. Compilar e gerar o .jar**

```
mkdir -p classes
javac -classpath $(hadoop classpath) -d classes CorrelacaoTemperatura.java
jar cf correlacao.jar -C classes .
```

**4. Enviar o CSV para o HDFS e conferir se chegou inteiro**

```
hdfs dfs -mkdir -p /forestfire/entrada
hdfs dfs -put -f forestfireinput.csv /forestfire/entrada/
wc -l forestfireinput.csv
hdfs dfs -cat /forestfire/entrada/forestfireinput.csv | wc -l
```

Os dois `wc -l` precisam dar o mesmo número (517).

**5. Rodar o MapReduce**

```
hdfs dfs -rm -r -f /forestfire/saida
hadoop jar correlacao.jar CorrelacaoTemperatura /forestfire/entrada /forestfire/saida
```

No final aparecem os contadores. O certo é `LIDAS=517` e `VALIDAS=517`.

**6. Ver o resultado**

```
hdfs dfs -cat /forestfire/saida/part-r-00000
```

Para sair do container: `exit`.

## Resultado

```
temp x 01_X      r = -0.0513   (n = 517)
temp x 02_Y      r = -0.0241   (n = 517)
temp x 03_month  r = +0.3688   (n = 517)
temp x 04_day    r = +0.0522   (n = 517)
temp x 05_FFMC   r = +0.4315   (n = 517)
temp x 06_DMC    r = +0.4696   (n = 517)
temp x 07_DC     r = +0.4962   (n = 517)
temp x 08_ISI    r = +0.3943   (n = 517)
temp x 10_RH     r = -0.5274   (n = 517)
temp x 11_wind   r = -0.2271   (n = 517)
temp x 12_rain   r = +0.0695   (n = 517)
temp x 13_area   r = +0.0978   (n = 517)
```

- A correlação mais forte é com a umidade **RH (-0,53)**: quanto mais quente, mais seco o ar.
- **DC, DMC, FFMC e ISI** (índices de secura da vegetação) têm correlação positiva fraca (0,39 a 0,50).
- **month (+0,37)**: os meses de verão são mais quentes.
- X, Y, day, wind, rain e area quase não têm correlação linear com a temperatura.
