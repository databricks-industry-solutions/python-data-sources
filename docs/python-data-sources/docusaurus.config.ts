import path from 'path';
import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'Python Data Sources',
  tagline: 'A collection of custom PySpark data source connectors for various formats.',
  favicon: 'img/logo.svg',

  url: 'https://databricks-industry-solutions.github.io',
  baseUrl: '/python-data-sources/',
  trailingSlash: true,

  organizationName: 'databricks-industry-solutions',
  projectName: 'python-data-sources',

  onBrokenLinks: 'throw',
  onDuplicateRoutes: 'throw',
  onBrokenAnchors: 'throw',

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },
  themes: ['@docusaurus/theme-mermaid'],

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  plugins: [
    async () => ({
      name: 'fix-vscode-languageserver-types-resolution',
      configureWebpack() {
        // theme-mermaid → mermaid → langium → vscode-languageserver-types.
        // vscode-languageserver-types defaults to UMD (main.js) which triggers
        // "Critical dependency: require function is used in a way in which
        // dependencies cannot be statically extracted". Force ESM build instead.
        const vscodeTypesPath = path.join(
          __dirname,
          'node_modules/vscode-languageserver-types/lib/esm/main.js',
        );
        return {
          resolve: {
            alias: {
              'vscode-languageserver-types': vscodeTypesPath,
            },
          },
        };
      },
    }),
    async (context, options) => {
      return {
        name: 'docusaurus-plugin-tailwindcss',
        configurePostCss(postcssOptions) {
          postcssOptions.plugins = [
            require('tailwindcss'),
            require('autoprefixer'),
          ];
          return postcssOptions;
        },
      };
    },
    'docusaurus-plugin-image-zoom',
    'docusaurus-lunr-search',
    [
      // TODO: pinned to pre-release alpha; revisit when a stable 2.0.0 is published
      // https://www.npmjs.com/package/@signalwire/docusaurus-plugin-llms-txt
      '@signalwire/docusaurus-plugin-llms-txt',
      {
        markdown: {
          enableFiles: true,
          relativePaths: true,
          includeBlog: false,
          includePages: true,
          includeDocs: true,
          includeVersionedDocs: false,
          excludeRoutes: [],
        },
        llmsTxt: {
          enableLlmsFullTxt: true,
          includeBlog: false,
          includePages: true,
          includeDocs: true,
          excludeRoutes: [],

          siteTitle: 'Python Data Sources',
          siteDescription: 'A collection of custom PySpark data source connectors for various formats.',

          autoSectionDepth: 1,
          autoSectionPosition: 100,

          sections: [
            {
              id: 'getting-started',
              name: 'Getting Started',
              description: 'Installation and motivation for Python Data Sources',
              position: 1,
              routes: [
                { route: '/python-data-sources/docs/installation' },
                { route: '/python-data-sources/docs/motivation' },
              ],
            },
            {
              id: 'reference',
              name: 'Reference',
              description: 'API reference and technical documentation',
              position: 2,
              routes: [
                { route: '/python-data-sources/docs/reference/**' },
              ],
            },
            {
              id: 'demos',
              name: 'Demos',
              description: 'Example notebooks and demos',
              position: 3,
              routes: [
                { route: '/python-data-sources/docs/demos' },
              ],
            },
            {
              id: 'home',
              name: 'Home',
              description: 'Python Data Sources homepage',
              position: 0,
              routes: [
                { route: '/python-data-sources/' },
                { route: '/python-data-sources/index' },
              ],
            },
          ],
        },
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/databricks-industry-solutions/python-data-sources/tree/main/docs/python-data-sources/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: 'Python Data Sources',
      logo: {
        alt: 'Python Data Sources Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'search',
          position: 'right',
        },
        {
          href: 'https://github.com/databricks-industry-solutions/python-data-sources',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      links: [],
      copyright: `Copyright © ${new Date().getFullYear()} Databricks Industry Solutions. Docs built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.oneLight,
      darkTheme: prismThemes.oneDark,
    },
    zoom: {
      selector: 'article img',
      background: {
        light: '#F8FAFC',
        dark: '#F8FAFC',
      },
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
