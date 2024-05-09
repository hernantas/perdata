import 'dotenv/config'
import { DataSource } from './source'
import { getEnvironment } from './util/environment'

describe('Data Source', () => {
  const { CLIENT, HOST, PORT, USER, PASSWORD, DATABASE } = getEnvironment()

  it('Data source with valid options should make connection', () => {
    expect(async () =>
      new DataSource({
        client: CLIENT,
        host: HOST,
        port: PORT,
        user: USER,
        password: PASSWORD,
        database: DATABASE,
      }).close(),
    ).not.toThrow()
  })
})
