/**
* Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// See https://docusaurus.io/docs/site-config.html for all the possible
// site configuration options.

const BASEURL='/';

/* List of projects/orgs using your project for the users page */
const users = [
  {
    caption: 'Facebook',
    image: 'img/facebook.png',
    infoLink: 'https://www.facebook.com',
    pinned: true,
  },
];

const siteConfig = {
  title: 'LogDevice' /* title for your website */,
  tagline: 'Distributed storage for sequential data',
  url: 'https://logdevice.io' /* your website url */,
  baseUrl: BASEURL,
  cname: "logdevice.io",
  // Used for publishing and more
  projectName: 'LogDevice',
  organizationName: 'facebookincubator',
  headerLinks: [
    {doc: 'Overview', label: 'Docs'},
    {href: BASEURL+'api/annotated.html', label: 'API'},
    {page: 'help', label: 'Support'},
    {href: 'https://github.com/facebookincubator/LogDevice', label: 'GitHub'},
    {blog: true, label: 'Blog'},
  ],

  algolia: {
    apiKey: 'f0ece773627cb7003a57c0edd6ec7dd8',
    indexName: 'logdevice',
  },

  // do not try to merge .css from static/api into main site css.
  // That directory contains a Doxygen-generated API subtree.
  separateCss: ['static/api'],
  users,

  /* path to images for header/footer */
  headerIcon: 'img/facebook_logdevice_whitewordmark.png',
  footerIcon: 'img/logdevice.svg',
  favicon: 'img/favicon.png',
  disableHeaderTitle: true,

  /* colors for website */
  colors: {
    primaryColor: '#20232a',
    secondaryColor: '#3C5B9A',
  },

  // This copyright info is used in /core/Footer.js and blog rss/atom feeds.
  copyright:
    'Copyright \u00A9 ' +
    new Date().getFullYear() +
    ' Facebook',

  highlight: {
    // Highlight.js theme to use for syntax highlighting in code blocks
    theme: 'default',
  },

  gaTrackingId: 'UA-137238014-1',

  scripts: ['https://buttons.github.io/buttons.js'],

  /* On page navigation for the current documentation page */
  onPageNav: 'separate',

  /* Open Graph and Twitter card images */
  ogImage: 'img/logdevice_og.png',
  twitterImage: 'img/logdevice.png',

  repoUrl: 'https://github.com/facebookincubator/LogDevice',
};

module.exports = siteConfig;
