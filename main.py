from db.database import Database
from helper.WriteAJson import writeAJson
from dataset.produto_database import dataset

compras = Database(database="database", collection="produtos", dataset=dataset)
compras.resetDatabase()

result = compras.collection.aggregate([
    {"$lookup":
        {
            "from": "pessoas",  # outra colecao
            "localField": "cliente_id",  # chave estrangeira
            "foreignField": "_id",  # id da outra colecao
            "as": "cliente"  # nome da saida
        }
     },
    {"$group": {"_id": "$cliente", "total": {"$sum": "$total"} } }, # formata os documentos
    {"$sort": {"total": -1} },
    {"$unwind": '$_id'},
    {"$project": {
        "_id": 1,
        "cliente": 1,
        "desconto": {
            "$cond": {"if": {"$gte": ["$total", 10]}, "then": 0.1, "else": 0.05}
        }
    }}
     ]
)

writeAJson(result,"result")








