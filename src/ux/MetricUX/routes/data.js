var express = require('express');
var request = require('request');

var router = express.Router();

/* GET environment list */
router.get("/environments", function (req, res) {
    request.get('https://proxy/MetricUX/Data/GetEnvironments',
        function (error, response, body) { res.json(["xap-prod-co3b","xap-prod-bn1"]) });//JSON.parse(body)) });
});

/* GET machines in an environment*/
router.get("/machines", function (req, res) {
    request.get('https://proxy/MetricUX/Data/UpdateMachineList?environment='+req.query.environment,
        function (error, response, body) { res.json(JSON.parse(body)) });
});

router.post("/info", function (req, res) {
    request.post('https://proxy/MetricUX/Data/GetJSON',
    { form: { machineName: req.body.machineName, environmentName: req.body.environmentName, counterName: req.body.counterName, queryCommand: req.body.queryCommand, queryParameters: req.body.queryParameters, timeoutValue: 2500  } },
    function (error, response, body) {
        if (!error && response.statusCode == 200) {
            if (body[0] !== "<") {
                try {
                    res.json(JSON.parse(body));
                }
                catch (SyntaxException) {
                    res.json({ error: "syntax error" });
                }
            }
            else {
                res.json(body);
            }
        }
        else {
            res.json([]);
        }
    }
    );
});

router.post("/query", function (req, res) {
    request.post('https://proxy/MetricUX/Data/GetJSON',
    { form: { queryCommand: "query", counterName: req.body.counterName, queryParameters: req.body.queryParameters, environmentName: req.body.environmentName, timeoutValue: req.body.timeoutValue } },
    function (error, response, body) {
        if (!error && response.statusCode === 200) {
            res.json(JSON.parse(body));
        }
        else {
            res.json({ counterName: "Test" });
        }
    }
    );
});

module.exports = router;