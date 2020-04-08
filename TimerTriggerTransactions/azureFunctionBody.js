/* 

*/

async function asyncForEach(array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array);
  }
}



const moment = require('moment');
const metadata = require('../tmp/metadataInvoice_2020-04-07.json');



const csv = require('csv-parser')
const fs = require('fs')


function tipoVersamento(t) {
  return Boolean(t.RTp_BBT + t.RTp_BP + t.RTp_AD + t.RTp_OBEP + t.RTp_CP + t.RTp_PO + t.RTp_JIG + t.RTp_MYBK)
}
function selectCategory(t, contract_type, membership_id) {
  //B1:  Pagamenti con carta inferiori 50 EUR. Applicabile solamente per contratti di tipo B
  //B2:   Pagamenti effettuati su rete reti. Applicabile solamente per contratti di tipo B
  //A1 :  Pagamenti effettuati su POS fisici. Applicabile solamente per contratti di tipo A
  //G_<fiscal_code> : pagamenti che ricadono all'interno di un gruppo di cumulo definito dal codice fiscale del PSP
  var category = "G_" + membership_id;
  switch (contract_type) {
    case "A":
      if (!tipoVersamento) category = "A1";
      break;
    case "B":
      if (metadata.acquirers.includes(t.IDENTIFICATIVO_CANALE) && (parseInt(t.IMPORTO_VERSATO) <= 5000)) category = "B1";
      break;
  }
  console.log("categoy selected : " + category);
  return category;
}


/*
a result si s
 {
   id : 
   provider:
   count:
   category:
   contract_type:
   recipient:
 }
*/
function createOrUpdateResults(t) {
  // console.log(t);
  console.log("CF_PSP: " + t.CF_PSP);
  console.log("IDENTIFICATIVO_CANALE: " + t.IDENTIFICATIVO_CANALE);
  const pspName = t.CF_PSP;
  const channelName = t.IDENTIFICATIVO_CANALE;
  // look for metadata 
  try {
    const rule = metadata[pspName + "_" + channelName];
    console.log(JSON.stringify(rule).toString())
    category = selectCategory(t, rule.contract_type, rule.membership_id);

    const filteredResults = results.filter(element => {
      return (element.provider == pspName && element.category == category && element.recipient == rule.recipient_id)
    })

    console.log("filtered: " + JSON.stringify(filteredResults).toString());
    if (filteredResults.length < 1) {
      console.log("new result");
      var aResult = {
        "id": results.length,
        "provider": pspName,
        "abi": rule.abi,
        "count": 1,
        "category": category,
        "contract_type": rule.contract_type,
        "recipient": rule.recipient_id
      }
      results.push(aResult);
    } else {
      console.log("update result " + filteredResults[0].id)
      filteredResults[0].count = filteredResults[0].count++;
    }
  }
  catch (err) {
    console.log(err);
  }

}

async function updateTransactions(transactionArr) {
  const tmp = await asyncForEach(transactionArr, createOrUpdateResults);
  console.log("results end : " + JSON.stringify(results).toString());
  fs.writeFile('invoiceResults.json', JSON.stringify(results).toString(), function (err) {
    if (err) throw err;
    console.log('Saved!');
  });

}


const csv_transactions = process.argv[2];
const transactions = []
var results = []

fs.readFile('invoiceResults.json', 'utf8', (err, jsonString) => {
  if (err) {
    console.log("no such results file!");
    results = [];
    return
  }
  try {
    results = JSON.parse(jsonString)
  } catch (err) {
    throw err;
  }

  console.log('File data:', jsonString)
})


fs.createReadStream(csv_transactions)
  .pipe(csv())
  .on('data', (data) => transactions.push(data))
  .on('end', () => {
    // console.log(contracts);
    const res = updateTransactions(transactions);
  });
