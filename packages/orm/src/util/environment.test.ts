import 'dotenv/config'
import { getEnvironment } from './environment'

describe('Environment variable', () => {
  it('Environment variable should be created, configured, and loaded correctly', () => {
    // create env config before running test
    expect(() => getEnvironment()).not.toThrow()
  })
})
