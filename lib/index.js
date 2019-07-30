const fs = require("fs");
const _ = require('lodash');
const parse = require("csv-parse");

const campaigns = [
    'Creative Contest',
    'LinkedIn Campaign',
    'Twitter Campaign',
    'Telegram Campaign',
    'Facebook Campaign',
    'Youtube Campaign',
    'Translation Campaign',
    'Peepeth Campaign',
    'Reddit Campaign',
    'Signature Campaign',
    'Bitcointalk Campaign'
];

const weekNos = _.range(22, 53).map(weekNo => `Week ${weekNo}`);

function parseFormData() {
    return new Promise((resolve) => {
        const cvs = fs.readFileSync("./input.csv");
        const output = [];
        parse(cvs, {
            trim: true,
            skip_empty_lines: true,
            columns: true
        })
            .on('readable', function () {
                let record;
                while ((record = this.read())) {
                    output.push(record)
                }
            })
            // When we are done, test that the parsed output matched what expected
            .on('end', function () {
                resolve(output);
            });
    });
}

function parseCampaigns(groupedWeek) {
    return groupedWeek.reduce((acc, cur) => {
        cur['Campaign Name'].split(';').forEach(campaign => {

            let normalizedCampaign = campaign;
            if (campaign === 'Facebook' || campaign === 'Facebook campaign') {
                normalizedCampaign = 'Facebook Campaign'
            }



            if (!campaigns.includes(normalizedCampaign)) {
                console.warn(`Invalid Campaign "${normalizedCampaign}"`);
                return;
            }

            acc[normalizedCampaign] = {done: true};
        });
        return acc;
    }, {});
}

function parseWeekNo(weekNo) {
    let result;

    const parsed = parseInt(weekNo);
    if (isNaN(parsed)) {
        result = weekNo.substring(0, 7);
    } else {
        switch (parsed) {
            case 1:
                result = 'Week 23';
                break;
            case 2:
                result = 'Week 24';
                break;
            case 3:
                result = 'Week 25';
                break;
            default:
                throw Error(`Could not parse weekNo "${parsed}"`);
        }
    }

    if (!weekNos.includes(result)) {
        throw Error(`Invalid weekNo "${result}"`);
    }

    return result;
}

function transformData(input) {
    const groupedByAddr = _.groupBy(input, 'ERC-20 Wallet Address');

    return Object.keys(groupedByAddr).map(addr => {
        const groupedWeek = _.groupBy(groupedByAddr[addr], 'Week Number');
        return {
            address: addr,
            weeks: Object.keys(groupedWeek).map(weekNo => {
                return {
                    weekNo: weekNo,
                    parsedWeekNo: parseWeekNo(weekNo),
                    campaigns: parseCampaigns(groupedWeek[weekNo])
                }
            })
        };
    });
}

function printStatistics(transformedData) {
    let totalCampaigns = 0;
    for (const member of transformedData) {
        for (const week of member.weeks) {
            totalCampaigns += Object.keys(week.campaigns).length
        }
    }

    console.log(`Total Bounty Members: ${transformedData.length}, Total Campaigns: ${totalCampaigns}`);
}

(async () => {
    const parsedData = await parseFormData();
    const transformedData = transformData(parsedData);

    printStatistics(transformedData);

    const output = {
        created: new Date().toISOString(),
        weekNos: weekNos,
        campaigns: campaigns,
        data: transformedData,
    };
    fs.writeFileSync('./output.json', JSON.stringify(output, null, 2));
})();
