//'use strict';

//https://docs.aws.amazon.com/pt_br/ses/latest/dg/notification-contents.html

/*const DynamoDBClient = require("aws-sdk/client-dynamodb") ;
const DynamoDBDocumentClient = require("@aws-sdk/lib-dynamodb");
const DynamoDB = require("aws-sdk/lib-dynamodb") ;*/

/*const AWS = require('aws-sdk');
var ddb = new AWS.DynamoDB({});*/
const { DynamoDB } = require('@aws-sdk/client-dynamodb');
var ddb = new DynamoDB({});

const crypto = require('crypto');

async function persistMail(messageId, send_timestamp, from, to, subject, type, subtype, status_timestamp) {
  //Atualiza/Insere item atual
  await ddb.putItem({
    TableName: 'ses_mail',
    Item: {
      "MessageId": {
        "S": messageId
      },
      "send_timestamp": {
        "S": send_timestamp
      },
      "from": {
        "S": from
      },
      "to": {
        "S": to
      },
      "subject": {
        "S": subject
      },
      "type": {
        "S": type
      },
      "subtype": {
        "S": subtype
      },
      "status_timestamp": {
        "S": status_timestamp
      }
    }

  })/*.promise()*/;

  //Insere LOG
  await ddb.putItem({
    TableName: 'ses_detail',
    Item: {
      "uuid": {
        "S": crypto.randomUUID()
      },
      "MessageId": {
        "S": messageId
      },
      "type": {
        "S": type
      },
      "subtype": {
        "S": subtype
      },
      "status_timestamp": {
        "S": status_timestamp
      }
    }
  })/*.promise()*/;
}

module.exports.handler = async (event) => {
  if (typeof event.Records !== "undefined") {

    try {
      await Promise.all(event.Records.map(async (record) => {
        if (typeof record.EventSource !== "undefined") {
          if (record.EventSource == "aws:sns") {
            let message = JSON.parse(record.Sns.Message);

            if (typeof message !== "undefined") {

              let eventTimeStamp = undefined;
              let subtype = "";

              switch (message.eventType) {
                case "Bounce": eventTimeStamp = message.bounce.timestamp; subtype = message.bounce.bounceType + " - " + message.bounce.bounceSubType; break;
                case "Complaint": eventTimeStamp = message.complaint.timestamp; break;
                case "Delivery": eventTimeStamp = message.delivery.timestamp; break;
                case "Send": eventTimeStamp = message.mail.timestamp; break;
                case "Reject": eventTimeStamp = message.reject.timestamp; break;
                case "Open": eventTimeStamp = message.open.timestamp; break;
                case "Click": eventTimeStamp = message.click.timestamp; break;
                case "Rendering Failure": eventTimeStamp = message.failure.timestamp; break;
                case "DeliveryDelay": eventTimeStamp = message.deliveryDelay.timestamp; break;
                case "Subscription": eventTimeStamp = message.subscription.timestamp; break;
              }

              if (message.eventType == "Bounce") {
                if (message.bounce.bounceType == "Permanent") {
                  //Add to OnAccountSuppressionList - Automático
                }
                else if (message.bounce.bounceType == "Transient") {


                }
              } else if (message.eventType == "Send") {
                //console.log("SEND To: %s. Subject: %s. Timestamp: %s. MessageID: %s", message.mail.commonHeaders.to[0], message.mail.commonHeaders.subject, message.mail.timestamp, message.mail.messageId);
                //await persist(message.mail.messageId, message.mail.timestamp, message.mail.commonHeaders.from[0], message.mail.commonHeaders.to[0], message.mail.commonHeaders.subject, message.eventType, message.mail.timestamp);

              }
              else if (message.eventType == "Open") {
                //console.log("OPEN To: %s. Subject: %s. Timestamp: %s. MessageID: %s", message.mail.commonHeaders.to[0], message.mail.commonHeaders.subject, message.mail.timestamp, message.mail.messageId);
                //await persist(message.mail.messageId, message.mail.timestamp, message.mail.commonHeaders.from[0], message.mail.commonHeaders.to[0], message.mail.commonHeaders.subject, message.eventType, message.open.timestamp);
              }
              else if (message.eventType == "Complaint") {
                //              console.log("COMPLAINT To: %s. Subject: %s. Timestamp: %s. MessageID: %s", message.mail.commonHeaders.to[0], message.mail.commonHeaders.subject, message.mail.timestamp, message.mail.messageId);

              }
              else if (message.eventType == "Delivery") {
                //              console.log("DELIVERY To: %s. Subject: %s. Timestamp: %s. MessageID: %s", message.mail.commonHeaders.to[0], message.mail.commonHeaders.subject, message.mail.timestamp, message.mail.messageId);
              }
              else if (message.eventType == "Subscription") {
                subtype = JSON.stringify(message.subscription.newTopicPreferences.topicSubscriptionStatus);
/*                if (message.subscription.newTopicPreferences.unsubscribeAll == true) {
                  subtype = "OPT_OUT";
                }
                else  {
                  subtype = "OPT_IN";
                }*/
              }
              else {
                console.error("Message type desconhecido. %s", JSON.stringify(event));
              }

              await persistMail(message.mail.messageId, message.mail.timestamp, message.mail.commonHeaders.from[0], message.mail.commonHeaders.to[0], message.mail.commonHeaders.subject, message.eventType, subtype, eventTimeStamp);
            }
            else {
              console.error("Objeto message inválido. %s", JSON.stringify(record));
            }

          }
        }
      }));
    }
    catch (err) {
      console.error(err);
    }
  }

  return {};
};
