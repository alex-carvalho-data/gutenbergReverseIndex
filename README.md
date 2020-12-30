# Indice Reverso - Textos do projeto Gutenberg

## Objetivos
* Ler todos os arquivos de textos e criar um dicionario no formato:  
  Formato:  
  (palavra, idPalavra)
    
* Criar um indice reverso aonde a key seja o id da Palavra e o value seja uma 
  lista dos documentos aonde existe ocorrencia da Palavra.  
  Formato:
  (palavraId, [docId1, docId2, docIdN])
  
## Ambiente utilizado
Para a solucao deste problema foi utilizada a Hortonworks SandBox 2.6.5.  
Esta Sandbox e uma Oracle Virtual Box que oferece um cluster com apenas uma 
maquina, que serve para o desenvolvimento e testes, para posterior deploy num 
cluster aonde ocorrera o processamento em paralelo.  
(https://www.cloudera.com/downloads/hortonworks-sandbox/hdp.html)  

### Versoes
* Python 2.7.5  
* HDFS 2.7.3  
* Spark 2.3.0  
* HDP 2.6.5
  
## Steps
1. Carregar arquivos no HDFS  
2. Gerar dicionario de palavras
3. Criar Indice Reverso

### 1. Carregando os arquivos no HDFS
1.1. Criar diretorio no maquina do cluster para armazenar os arquivos 
referentes ao projeto
```shell
ssh -p 2222 maria_dev@localhost 'mkdir ~/project22'
# password: maria_dev
```
1.2. Copiar pasta com os datasets para o maquina do cluster 
```shell
scp -P 2222 -r ~/temp/dataset maria_dev@localhost:~/project22
# password: maria_dev
```
1.4. Logar na maquina do cluster Hadoop
```shell
ssh -p 2222 maria_dev@localhost
# password: maria_dev
```
1.5. Criar pastar no HDFS para armazenar o dataset  
```shell
hadoop fs -mkdir -p project22/output
```
1.6. Carregar os arquivos no HDFS do cluster
```shell
hadoop fs -copyFromLocal ~/project22/dataset project22/dataset
```
### Git Repository clone
Para a execucao dos Step 2 e 3 e necessario copiar este 
repositorio Git para o servidor do cluster que executara este
processo.
* Conectar no cluster Hadoop
```shell
ssh -p 2222 maria_dev@localhost
# password: maria_dev
```
* Clonar o repositorio
```shell
git clone https://github.com/alexcarvalhodata/gutenbergReverseIndex.git
```
### 2. Gerando dicionario de palavras

### 3. Criando Indice Reverso