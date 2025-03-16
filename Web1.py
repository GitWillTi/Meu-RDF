import mysql.connector
from rdflib import Graph, URIRef, Literal, Namespace

# Conectar ao banco de dados MySQL
db_connection = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="",
    database="cana_acucar"
)

cursor = db_connection.cursor()

# Criar um grafo RDF
g = Graph()
CANA = Namespace("http://example.org/cana#")
USOS = Namespace("http://example.org/usos#")
PRODUCAO = Namespace("http://example.org/producao#")
IMPATOS = Namespace("http://example.org/impactos#")

# Adicionar dados da tabela cana
cursor.execute("SELECT * FROM cana")
for (id, nome, familia, tipo, origem) in cursor.fetchall():
    g.add((URIRef(CANA[nome.replace(" ", "_")]), 
            URIRef("http://example.org/prop#nome"), Literal(nome)))
    g.add((URIRef(CANA[nome.replace(" ", "_")]), 
            URIRef("http://example.org/prop#familia"), Literal(familia)))
    g.add((URIRef(CANA[nome.replace(" ", "_")]), 
            URIRef("http://example.org/prop#tipo"), Literal(tipo)))
    g.add((URIRef(CANA[nome.replace(" ", "_")]), 
            URIRef("http://example.org/prop#origem"), Literal(origem)))

# Adicionar dados da tabela producao
cursor.execute("SELECT * FROM producao")
for (id, ano, quantidade, unidade, id_cana) in cursor.fetchall():
    g.add((URIRef(PRODUCAO[f"produzido_{id}"]), 
            URIRef("http://example.org/prop#ano"), Literal(ano)))
    g.add((URIRef(PRODUCAO[f"produzido_{id}"]), 
            URIRef("http://example.org/prop#quantidade"), Literal(quantidade)))
    g.add((URIRef(PRODUCAO[f"produzido_{id}"]), 
            URIRef("http://example.org/prop#unidade"), Literal(unidade)))
    g.add((URIRef(PRODUCAO[f"produzido_{id}"]), 
            URIRef("http://example.org/prop#cana"), URIRef(CANA[nome.replace(" ", "_")])))
    
# Adicionar dados da tabela usos
cursor.execute("SELECT * FROM usos")
for (id, descricao) in cursor.fetchall():
    g.add((URIRef(USOS[f"uso_{id}"]), 
            URIRef("http://example.org/prop#descricao"), Literal(descricao)))

# Adicionar dados da tabela impactos
cursor.execute("SELECT * FROM impactos")
for (id, tipo, descricao) in cursor.fetchall():
    g.add((URIRef(IMPATOS[f"impacto_{id}"]), 
            URIRef("http://example.org/prop#tipo"), Literal(tipo)))
    g.add((URIRef(IMPATOS[f"impacto_{id}"]), 
            URIRef("http://example.org/prop#descricao"), Literal(descricao)))

# Salvar o grafo RDF em um arquivo
g.serialize(destination='cana_acucar.rdf', format='xml')

# Fechar a conexão
cursor.close()
db_connection.close()

print("RDF gerado com sucesso!")