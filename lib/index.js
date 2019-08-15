const fs = require("fs");
const _ = require('lodash');
const parse = require("csv-parse");
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

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

const weekNos = _.range(20, 53).map(weekNo => `Week ${weekNo}`);

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

            acc[normalizedCampaign] = acc[normalizedCampaign] || {done: true, count: 0};
            acc[normalizedCampaign].count++;
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
            address: addr.trim(),
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

function createStats(transformedData) {
    const totalTokensByCampaign = {
        'Creative Contest': 50000,
        'LinkedIn Campaign': 50000,
        'Twitter Campaign': 50000,
        'Telegram Campaign': 50000,
        'Facebook Campaign': 50000,
        'Youtube Campaign': 50000,
        'Translation Campaign': 50000,
        'Peepeth Campaign': 25000,
        'Reddit Campaign': 50000,
        'Signature Campaign': 25000,
        'Bitcointalk Campaign': 25000
    };

    const tokenBasedCampaigns = ['Youtube Campaign'];


    let totalCampaigns = 0;
    for (const member of transformedData) {
        for (const week of member.weeks) {
            totalCampaigns += Object.keys(week.campaigns).length
        }
    }

    const stats = transformedData.reduce((stats, member) => {
        member.weeks.forEach(week => {
            Object.keys(week.campaigns).forEach(campaign => {
                if (week.campaigns[campaign].done) {
                    stats.campaignTotals[campaign] = stats.campaignTotals[campaign] || {count: 0, stakes: 0};
                    stats.campaignTotals[campaign].count += week.campaigns[campaign].count;
                    if (!tokenBasedCampaigns.includes(campaign)) {
                        stats.campaignTotals[campaign].stakes += 5*week.campaigns[campaign].count;
                    }
                }

                stats.byMemberAndCampaign[member.address] = stats.byMemberAndCampaign[member.address] || {};
                stats.byMemberAndCampaign[member.address][campaign] = stats.byMemberAndCampaign[member.address][campaign] || {
                    count: 0,
                    stakes: 0
                };
                stats.byMemberAndCampaign[member.address][campaign].count += week.campaigns[campaign].count;
                if (!tokenBasedCampaigns.includes(campaign)) {
                    stats.byMemberAndCampaign[member.address][campaign].stakes += 5*week.campaigns[campaign].count;
                }

                stats.byMember[member.address] = stats.byMember[member.address] || {count: 0, stakes: 0};
                stats.byMember[member.address].count += week.campaigns[campaign].count;
                if (!tokenBasedCampaigns.includes(campaign)) {
                    stats.byMember[member.address].stakes += 5*week.campaigns[campaign].count;
                }
            });
        });

        return stats;
    }, {
        campaignTotals: {},
        byMemberAndCampaign: {},
        byMember: {},
        totalTokensByCampaign,
        totalCampaigns
    });

    Object.keys(stats.byMemberAndCampaign).forEach(member => {
        Object.keys(stats.byMemberAndCampaign[member]).forEach(campaignName => {
            const campaign = stats.byMemberAndCampaign[member][campaignName];
            if (!tokenBasedCampaigns.includes(campaignName)) {
                campaign.percentage = Number(((campaign.stakes * 100) / stats.campaignTotals[campaignName].stakes).toFixed(2));
                campaign.effectivePercentage = campaign.percentage > 5 ? 5 : campaign.percentage;
                campaign.tokens = campaign.effectivePercentage * (totalTokensByCampaign[campaignName] / 100)
            } else {
                campaign.tokens = campaign.count * 500;
            }
        });
    });

    stats.byMemberTotalTokens = Object.keys(stats.byMemberAndCampaign).reduce((totals, member) => {
        totals[member] = _.sum(Object.keys(stats.byMemberAndCampaign[member]).map(campaignName => {
            return stats.byMemberAndCampaign[member][campaignName].tokens;
        }));
        return totals;
    }, {});

    stats.detailedExport = _.flatten(Object.keys(stats.byMemberAndCampaign).map(member => {
        return Object.keys(stats.byMemberAndCampaign[member]).map(campaignName => {
            const clonedCampaign = {...stats.byMemberAndCampaign[member][campaignName]};

            clonedCampaign.ethAddress = member;
            clonedCampaign.campaign = campaignName;

            return clonedCampaign;
        });
    }));

    return stats;
}

function printStatistics(stats) {
    console.log(`Total Bounty Members: ${Object.keys(stats.byMemberTotalTokens).length}, Total Campaign tasks: ${stats.totalCampaigns}`);

    console.table(stats.campaignTotals);
    console.log('Stakes by member');
    console.table(stats.byMember);
    let sum=0;
    Object.keys(stats.byMemberAndCampaign).forEach(member => {
        console.log('Ethereum address:', member, 'Total tokens:', stats.byMemberTotalTokens[member]);
        console.table(stats.byMemberAndCampaign[member]);
        sum+=stats.byMemberTotalTokens[member];
    });
    console.log(`Total ASR tokens: ${sum}`);
    console.table(stats.byMemberTotalTokens);
    console.table(stats.detailedExport);
}

function exportAsCsv(data) {
    const csvWriter = createCsvWriter({
        path: 'output-token-distribution.csv',
        header: [
            {id: 'ethAddress', title: 'ETHAddress'},
            {id: 'count', title: 'Count'},
            {id: 'stakes', title: 'Stakes'},
            {id: 'percentage', title: 'Percentage'},
            {id: 'effectivePercentage', title: 'EffectivePercentage'},
            {id: 'percentage', title: 'Percentage'},
            {id: 'tokens', title: 'Tokens'},
            {id: 'campaign', title: 'Campaign'},
        ]
    });

    return csvWriter.writeRecords(data);
}

(async () => {
    const parsedData = await parseFormData();
    const transformedData = transformData(parsedData);

    const stats = createStats(transformedData);
    printStatistics(stats);

    const output = {
        created: new Date().toISOString(),
        weekNos: weekNos,
        campaigns: campaigns,
        data: transformedData,
    };
    fs.writeFileSync('./output.json', JSON.stringify(output, null, 2));
    fs.writeFileSync('./output-detailed-export.json', JSON.stringify(stats.detailedExport, null, 2));
    fs.writeFileSync('./output-token-distribution.json', JSON.stringify(stats.byMemberTotalTokens, null, 2));
    await exportAsCsv(stats.detailedExport);
})();
