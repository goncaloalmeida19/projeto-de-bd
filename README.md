# Bases de Dados - Trabalho Prático

a. Descrição do sistema
    Esta plataforma permite a comercialização de diferentes tipos de equipamentos eletrónicos (“computers”, “televisions” e “smartphones”). Cada produto é vendido por uma empresa específica e é caracterizado por um identificador único (id) e outros atributos específicos do produto além dos genéricos “name”, “stock”, “description” e “price”.
    Sempre que há atualização dos detalhes de um produto é criada uma nova versão que mantém os detalhes não alterados e os novos, sendo assim criado um histórico das versões anteriores.

b. Níveis de acesso dos utilizadores
    O tipo de utilizador com maior poder hierárquico é “admin”, com permissões de moderação e gestão da plataforma. Apenas um “admin” pode criar vendedores (“sellers”) e outros “admins” e criar campanhas promocionais. Outros tipos de utilizadores também têm acesso a operações exclusivas:
    Um user do tipo “seller” pode adicionar e atualizar os produtos que vende.
    Um user do tipo “buyer” pode efetuar compras, deixar feedback (“rating”) de um produto que tenha comprado e subscrever a campanhas promocionais.
    Qualquer utilizador pode consultar estatísticas da plataforma relativas a vendas e campanhas promocionais.
    
c. Pré requisitos
    Nesta secção é apresentada uma lista de pré-requisitos de instalação que devem ser atendidos antes da instalação.
    • Software necessário
        i. PostgresSQL versão 14
        ii. Python versão 3.9+
        iii. Postman versão 9.18.2
    • Python packages
Para a utilização do script “api.py” são necessários os seguintes packages:
        i. Flask versão 2.1.2;
        ii. psycopg2-binary versão 2.9.3;
        iii. PyJWT versão 2.3.0;
        iv. cryptography versão 37.0.2;
        
d. Processo de instalação
    Após verificar que reúne todos os requisitos, pode proceder à instalação. Para criar a DB deve executar o script “dbproj_create.sql” na linha de comandos da seguinte forma:
    Este script:
        • Cria a DB com o nome “dbproj”;
        • Cria um utilizador com o nome “projuser” e a password “projuser”;
        • Cria as tabelas e funções;
        • Insere alguns dados-exemplo nas tabelas.
e. Desinstalação
    Para desinstalar a DB “dbproj” e remover o utilizador “projuser”, execute o script “dbproj_drop.sql”:
        psql -h localhost -U postgres -f dbproj_create.sql postgres
        psql -h localhost -U postgres -f dbproj_drop.sql postgres

f. Segurança da base de dados
    A palavra-passe usada para aceder à base de dados encontra-se encriptada usando uma chave fernet fornecida num ficheiro. É desencriptada em cada momento de conexão.
    O utilizador criado para aceder à base de dados tem permissões limitadas: apenas pode conectar-se e realizar as operações select, update e insert, que são as necessárias para as funcionalidades implementadas. Foi também certificado que novos utilizadores criados não adquirem automaticamente permissões.
    As passwords dos utilizadores são armazenadas na base de dados de forma encriptada.
    
g. Autenticação de utilizadores
    A autenticação é realizada através de tokens JWT. Ao realizar o login é gerada uma token com o ID de utilizador, usando uma chave associada à aplicação Flask, com o momento de criação definido e com uma audiência definida como o nome de sessão da aplicação (o que torna a token descodificável apenas por essa sessão). É também definido um momento de expiração para a token, pelo que uma sessão de login de um utilizador é válida por 20 minutos.
    A partir do token, ao ser incluído no header das chamadas, é extraído o ID de utilizador, usado para verificar se tem autorização para realizar a operação pedida.

h. Controlo de concorrência e transações
    Tendo em conta a possibilidade de vários utilizadores acederem em simultâneo às mesmas informações da base de dados, foram implementados locks para evitar situações problemáticas.
    Para casos em que é necessária informação de toda a tabela foi realizado “lock table” (por exemplo, ao obter o valor de “max(campaign_id)”, é necessário evitar a inserção de novas linhas até ao fim da transação).
    Ao realizar uma compra é feito lock da tabela “products” para evitar possíveis deadlocks quando compradores acedem aos mesmos produtos por ordens diferentes. Foi também tido em conta que a instrução “UPDATE” faz lock às linhas atualizadas implicitamente (por exemplo, ao subscrever uma campanha, o número de cupões é decrementado em 1, mas não há risco desta operação ser realizada na falta de cupões suficientes para vários compradores que tentam subscrever em simultâneo).
    Finalmente, transações que apenas envolvem “SELECT”s foram definidas como “read only”.

i. Notificações
    Foram implementados triggers para notificar utilizadores em relação aos seguintes eventos:
        • É colocada/respondida uma questão em relação a um produto;
            o Vendedor do produto e utilizador que escreveu a questão a ser respondida são notificados do ID da nova questão e do seu conteúdo.
        • É realizada uma venda;
            o Vendedores cujos produtos foram comprados são notificados do ID da venda e do seu total.
        • É deixado feedback (rating e comentário) em relação a um produto;
            o Vendedor do produto é notificado da classificação atribuída e comentário associado.
