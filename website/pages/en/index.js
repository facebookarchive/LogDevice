/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const siteConfig = require(process.cwd() + '/siteConfig.js');

function imgUrl(img) {
  return siteConfig.baseUrl + 'img/' + img;
}

function docUrl(doc, language) {
  return siteConfig.baseUrl + 'docs/' + (language ? language + '/' : '') + doc;
}

function pageUrl(page, language) {
  return siteConfig.baseUrl + (language ? language + '/' : '') + page;
}

class Button extends React.Component {
  render() {
    return (
      <div className="pluginWrapper buttonWrapper">
        <a className="button" href={this.props.href} target={this.props.target}>
          {this.props.children}
        </a>
      </div>
    );
  }
}

Button.defaultProps = {
  target: '_self',
};

const SplashContainer = props => (
  <div className="homeContainer">
    <div className="homeSplashFade">
      <div className="wrapper homeWrapper">{props.children}</div>
    </div>
  </div>
);

const Logo = props => (
  <div className="projectLogo">
    <img src={props.img_src} />
  </div>
);

const ProjectTitle = props => (
  <div>
  <h2 className="projectTitle">
  {siteConfig.tagline}
  </h2>
    <small>LogDevice is a scalable distributed log storage system that offers durability, high availability, and total order of records under failures. It is designed for a variety of workloads including event streaming, replication pipelines, transaction logs, and deferred work journals.</small>
  </div>
);

const PromoSection = props => (
  <div className="section promoSection">
    <div className="promoRow">
      <div className="pluginRowBlock">{props.children}</div>
    </div>
  </div>
);

class HomeSplash extends React.Component {
  render() {
    let language = this.props.language || '';
    return (
      <SplashContainer>
        <div className="inner">
          <ProjectTitle />
          <PromoSection>
            <Button href={docUrl('Overview.html', language)}>Get Started</Button>
            <Button href="https://github.com/facebookincubator/LogDevice">GitHub</Button>
          </PromoSection>
        </div>
      </SplashContainer>
    );
  }
}

const Block = props => (
  <Container
    padding={['bottom']}
    id={props.id}
    background={props.background}>
    <GridBlock align="left" contents={props.children} layout={props.layout} />
  </Container>
);

const Features = props => (
  <Block layout="fourColumn">
    {[
      {
        content: 'Store up to a million logs on a single cluster.',
        title: 'Scalable',
      },
      {
        content: 'Log records are replicated across multiple nodes and failure domains.',
        title: 'Fault-tolerant',
      },
      {
        content: 'LogDevice handles full node/rack failures like a breeze. It is also resilient to slow or degraded nodes.',
        title: 'Highly available',
      },
      {
        content: 'A storage engine optimized for logs supports both SSD and HDD storage.',
        title: 'Efficient',
      },
    ]}
  </Block>
);


const FeatureCallout = props => (
  <div
    className="productShowcaseSection paddingBottom"
    style={{textAlign: 'center'}}>
	<h2>Feature Callout</h2>
	<MarkdownBlock>These are features of this project</MarkdownBlock>
  </div>
);


const LearnHow = props => (
  <Block background="light">
    {[
      {
        content: 'Talk about learning how to use this',
        image: imgUrl('logdevice.svg'),
        imageAlign: 'right',
        title: 'Learn How',
      },
    ]}
  </Block>
);

const TryOut = props => (
  <Block id="try">
    {[
      {
        content: 'Talk about trying this out',
        image: imgUrl('logdevice.svg'),
        imageAlign: 'left',
        title: 'Try it Out',
      },
    ]}
  </Block>
);

const Description = props => (
  <Block background="dark">
    {[
      {
        content: '<p>LogDevice is designed from the ground up to serve many types of logs with high reliability and efficiency at scale. It is also highly tunable allowing each use case to be optimized for the right set of trade-offs in the durability-efficiency and consistency-availability space.</p><p>LogDevice is heavily used for high-durability low-latency streaming use cases as well as high-throughput machine-learning pipelines. The customizability that LogDevice offers makes it a versatile building block in many situations.</p>',
        title: '<span class="title">Designed from the ground up for data logging of all types.</span>',
      },
    ]}
  </Block>
);

const Showcase = props => {
  if ((siteConfig.users || []).length === 0) {
    return null;
  }
  const showcase = siteConfig.users
    .filter(user => {
      return user.pinned;
    })
    .map((user, i) => {
      return (
        <a href={user.infoLink} key={i}>
          <img src={user.image} alt={user.caption} title={user.caption} />
        </a>
      );
    });

  return (
    <div className="productShowcaseSection paddingBottom">
      <h2>{"Who's Using This?"}</h2>
      <p>This is who is currently using LogDevice</p>
      <div className="logos">{showcase}</div>
      <div className="more-users">
        <a className="button" href={pageUrl('users.html', props.language)}>
          More {siteConfig.title} Users
        </a>
      </div>
    </div>
  );
};

class Index extends React.Component {
  render() {
    let language = this.props.language || '';

    return (
      <div>
        <HomeSplash language={language} />
        <div className="mainContainer">
          <Features />
          <Description />
          <Showcase language={language} />
        </div>
      </div>
    );
  }
}

module.exports = Index;
