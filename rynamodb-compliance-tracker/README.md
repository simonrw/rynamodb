# Compliance tracker for rynamdob

This crate contains a web server capable of handling the metrics data submitted by the compliance tests component of `rynamodb`. 

The compliance tests output the following information:

* `errors`
* `failed`
* `skipped`
* `passed`
* `duration`

In addition we capture (from GitHub actions)

* `branch`
* `commit-sha`
* `committer`

and add the `uploaded` date.

## Initial implementation

### Storage

The plan is to use this project to drive features of `rynamodb`. Eventually this project will use DynamoDB to track the compliance results over time, and we will use `rynamodb` to develop against.

At the moment, the feature set of `rynamodb` is not sufficient to use in this way, so we use `sqlite` to store the compliance data.

Once the tracking is in place, we will use `rynamodb` to develop against while targeting DynamoDB. The eventual schema will probably look something like:

| Attribute name | Hash type |
| --             | --        |
| `branch`       | `HASH`    |
| `uploaded`     | `RANGE`   |
| `errors`       |           |
| `failed`       |           |
| `skipped`      |           |
| `passed`       |           |
| `duration`     |           |
| `commit-sha`   |           |
| `committer`    |           |

### Security

Since this will be a public server listening for requests from GitHub, we will put some rudimentary security in place. GitHub actions will have access to a secret, which will be passed in the header of the request to `rynamdob-compliance-tracker` and verified on the server. *Note*: in the future, we may use signed JWTs instead, but for now 🤷.


