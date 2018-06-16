var Module = require('./module');
var url = require('url');

class AdminModule extends Module {
  constructor (msm) {
    super(msm);
    if (msm.isProduction() && !msm.config.get('Admin.allowInProduction')) {
      throw new Exception('AdminModule not allowed in production environment.');
    }
    this.basePath = msm.config.get('Admin.basePath');
  }
  init (msm, next) {
    msm.app.use(msm.kernel.bodyParser.json({limit: '1mb'}));
    msm.app.use('/v1/admin', require('../../routes/v1/admin'));
  }
}

module.exports = AdminModule;
