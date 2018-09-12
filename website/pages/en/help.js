/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const siteConfig = require(process.cwd() + '/siteConfig.js');

function docUrl(doc, language) {
  return siteConfig.baseUrl + 'docs/' + (language ? language + '/' : '') + doc;
}

class Help extends React.Component {
  render() {
    let language = this.props.language || '';
    const supportLinks = [
      {
        content: `Learn more using the [documentation on this site.](${docUrl(
          'Overview.html'
        )})`,
        title: 'Browse Docs',
      },
      {
        content: 'Use [GitHub issues](https://github.com/facebookincubator/LogDevice/issues) to report bugs, issues and feature requests for the LogDevice codebase.',
        title: 'Report Issues',
      },
      {
        content: "Use the [LogDevice Users Facebook group](https://facebook.com/groups/logdevice.oss) for general questions and discussion about LogDevice.",
        title: 'Facebook Group',
      },
    ];

    return (
      <div className="docMainWrapper wrapper">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h2>Need help?</h2>
            </header>
            <p>Do not hesitate to ask questions if you are having trouble with LogDevice</p>
            <GridBlock contents={supportLinks} layout="threeColumn" />
          </div>
        </Container>
      </div>
    );
  }
}

module.exports = Help;
