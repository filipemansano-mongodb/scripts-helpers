const { MongoClient, ObjectId } = require('mongodb');

/**
 * Esta função realiza a atualização em massa de documentos em uma coleção do MongoDB (collectionB),
 * embutindo dados referenciados de outra coleção (collectionA). Utiliza uma abordagem de paginação
 * eficiente com um limite definido para processar grandes conjuntos de dados. A cada iteração, recupera 
 * uma página de documentos da collectionA e atualiza documentos correspondentes na collectionB, 
 * inserindo informações detalhadas do objeto.
 */
async function main() {
    const client = new MongoClient(process.env.MONGO_URL);

    try {
        await client.connect();
        const database = client.db("sample_analytics");

        // collection que contém as informações completas que está sendo referenciada
        const collectionA = database.collection("accounts");

        // collection que contém a chave estrangeria para a collectionA
        const collectionB = database.collection("transactions");

        const limit = 100;
        let page = null;
        let lastId = undefined;

        while (lastId === undefined || page.length > 0) {

            let query = lastId 
                ? { _id: { $gt: new ObjectId(lastId) } } 
                : {};

            console.log('Find records...');
            page = await collectionA.find(query).limit(limit).sort({_id: 1}).toArray();

            const pageLength = page.length;

            const changes = page.map(docA => {
                return {
                    updateMany: {
                        
                        filter: { 
                            account_id: docA.account_id  // chave estrangeira da collection B -> A
                            /**
                             * adicionar outras regras que identifica que esse 
                             * campo já foi atualizado
                             */
                        },
                        
                        // Substitua pelo seus campos
                        update: { 
                            $set: { 
                                account: {
                                    _id: docA._id,
                                    products: docA.products,
                                } 
                            } 
                        }
                    }
                };
            });

            console.log(`Running (${pageLength})... records`);
            if(pageLength > 0){
                const results = await collectionB.bulkWrite(changes);
                console.log(`Matched ${results.result.nMatched} documents`);
                console.log(`Modified ${results.result.nModified} documents`);
                console.log(`Errors ${results.result.writeErrors.length} documents`);

                if(results.result.writeErrors.length > 0){
                    console.error(results.result.writeErrors);
                }
            }
            
            lastId = page[pageLength - 1]?._id.toString();

            console.log('LastId:', lastId);
        }
    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}

main().catch(console.error);