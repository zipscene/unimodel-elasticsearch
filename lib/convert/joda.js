const moment = require('moment');

const JODA_ISO_STRING_FORMAT = 'yyyy-MM-ddHH:mm:ss.SSS';
exports.JODA_ISO_STRING_FORMAT = JODA_ISO_STRING_FORMAT;

const MOMENT_TO_JODA_FORMAT = 'YYYY-MM-DDHH:mm:ss.sss';
exports.MOMENT_TO_JODA_FORMAT = MOMENT_TO_JODA_FORMAT;

function toJodaFormat(date) {
	return moment(date).format(MOMENT_TO_JODA_FORMAT);
}
exports.toJodaFormat = toJodaFormat;

function fromJodaFormat(jodaStr) {
	return moment.utc(jodaStr, MOMENT_TO_JODA_FORMAT).toISOString();
}
exports.fromJodaFormat = fromJodaFormat;
