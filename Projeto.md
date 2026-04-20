# O que foi feito até então

Quando peguei para trabalhar com o Julius ele era um extrator de dados apenas, então a ideia era atualizar os códigos para que eles voltassem a extrair os dados das prefeituras. Como muita coisa havia mudado, atualizei quais empresas cada prefeitura estava utilizando no momento (última atualização no primeiro semestre de 2025) e recriei os códigos tendo enfoque as empresas, então foi um código para cada prefeitura que utilizava as seguintes empresas em seu portal da transparência: Portaltp, tectrilha e um cógio englobando agape+alphatec. Esses código se encontram na pasta src. Os arquivos dos endpoints estão na pasta data e os logs que são gerados ao rodar os códigos das empresas vão para a pasta logs.

Após isso, seguimos o caminho de fazer embedding nos dados. Primeiramente, foi escolhida a prefeitura de São Gabriel da Palha para ter os dados extraídos. Após isso, criei o arquivo extrator_convenios.py que se encontra na pasta src_IA e utilizei Selenium para quando eu rodasse o código, ele abrir nos Id's indicados (onde haviam pdfs à serem baixados), expandir a página, depois ir até a parte debaixo da página, clicar para expandir a tabela para aparecer todos os arquivos disponíveis, e a partir daí, baixa-los. Esse processo se repete até baixar todos os pdfs de todos os Id's. O código também faz verificação de quais arquivos já foram extraídos para que não houvesse arquivos duplicados. Os pdf's extraídos vão para a pasta documentos_convênios, onde para cada Id é criada uma pasta dentro dessa pasta. O banco de dados criado é convenios.db e fica na pasta bds, dentro de src_IA, e contém as informações que tem no site da prefeitura sobre cada convênio extraído.  

Após isso, brinquei um pouco com embeddins até ser decidido algumas coisas. Ficou decidida a troca de sqlite para duckdb, por uma facilidade de manipulação de contexto dos dados e a utilização do gpt azure para o chatbot. 

Após isso, criei o arquivo extrair_texto.py, que se encontra na pasta src_IA, para fazer OCR dos arquivos que precisavam de OCR após sereme extrtaídos como PDF, usando Tesseract. Como nem todos arquivos precisavam da utilização do OCR, há uma verificação que antecede a extração do texto de um arquivo, e arquivos que não precisavam de OCR, tinham seu texto extraído sem OCR. A saída desse código acontece em duckdb e é salva na pasta bds dentro da pasta geral GMJULIUS.

Após isso criei o arquivo OpenIA2.py, que se encontra  na pasta src_IA para testar algumas funcionalidades de busca textual utilizando o pt azure com os dados extraídos dos pdf's.

Após testes feitos, criei o arquivo portaltp_IA.py, que se encontra na pasta src_IA para extrair as tabelas no portal da transparência. Esse código segue o mesmo esquema da extração dos pdfs. Tive que verificar a extração de cada ID porque alguns endpoints eram mensais, outros anuais, então tive que adaptar a exção para cada caso. Por algum motivo que não lembro eu não coloquei para a saída ser em duckdb, entção a saído é em sqlite.

Criei alguns pipelines durante essa jornada, todos se encontram na pasta Pipelines. BancoDuckDB.py transforma o sqlite em duckdb. ColuneTokens.py cria colunas com quantidade de tokens, pois em algum momento estava tendo problema de estourar tokens. Id.py foi criado para que, após fazer alguns "appends' os ids estavam se repetindo, então esse pipeline resolve esse problema refazendo a coluna id. jsonToMongoDB.py foi criado pois no inicio, quando estava aprendendo sobre busca textual, estava utilizando o MongoDB para estudos, porém sem utilização no momento para o Julius. RemoveColumns.py e RenameColumns.py foram criados como facilitadores quando eu estava tendo dificuldade em fazer essas alterações em determinados momentos. sqlite_para_json.py, SQLiteToDuckDB.py e SqlTables_to_duckdb.py são alterações de banco de dados. Type.py foi criadp para alterar o tipo de determinadas colunas em SQLite. Unir_bds.py foi criada para fazer o append quando necessário.

Os arquivos que não aparecerem no GitHub, foram bloqueados pelo .env.

# Para o futuro

No momento estou focando em pesquisar e implementar melhorias na resposta do chatbot, pois ele está respondendo as perguntas de forma insatisfatória e acredito ser isso que irei focar até que o chatbot estaja respondendo de uma forma coerente para ser implementado como uma ferramenta real. Quando os resultados estiverem satisfatórios, irei concentrar meus esforços para generalizar para outras prefeituras do estados que utilizam a empresa portaltp no portal da transparência, e depois para as demais empresas, para que finalmente possa ser utilizado por servidores e ser uma ferramenta útil e atualizada com os dados públicos.

- Limpeza na tabela que foi feito OCR.
- Melhorar os comentários inseridos nas tabelas
- Fazer o chatbot responder de forma mais clara e eficiente
- Alterar a prefeitura???
