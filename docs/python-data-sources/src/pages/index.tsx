import Layout from '@theme/Layout';
import { JSX } from 'react';
import Button from '../components/Button';
import { Database, Radio, Image, FileText, Cog, Zap } from 'lucide-react';

const Hero = () => {
  return (
    <div className="px-4 md:px-10 min-h-screen flex flex-col justify-center items-center w-full">
      <div className="m-2">
        <img src="img/logo.svg" alt="Python Data Sources Logo" className="w-32 md:w-48" />
      </div>

      <h1 className="text-4xl md:text-5xl font-semibold text-center mb-6">
        Python Data Sources
      </h1>
      <p className="text-center text-gray-600 dark:text-gray-500 mb-4">
        Provided by{' '}
        <a
          href="https://github.com/databricks-industry-solutions"
          className="underline text-blue-500 hover:text-blue-700"
        >
          Databricks Industry Solutions
        </a>
      </p>
      <p className="text-lg text-center text-balance">
        A collection of custom PySpark data source connectors for formats and protocols
        that don't have a built-in Spark reader.
      </p>

      <div className="mt-12 flex flex-col space-y-4 md:flex-row md:space-y-0 md:space-x-4">
        <Button
          variant="secondary"
          outline={true}
          link="/docs/motivation"
          size="large"
          label="Motivation"
          className="w-full md:w-auto"
        />
        <Button
          variant="secondary"
          outline={true}
          link="/docs/installation"
          size="large"
          label="Installation"
          className="w-full md:w-auto"
        />
        <Button
          variant="secondary"
          outline={true}
          link="/docs/demos"
          size="large"
          label="Demos"
          className="w-full md:w-auto"
        />
        <Button
          variant="secondary"
          outline={true}
          link="/docs/reference"
          size="large"
          label="Reference"
          className="w-full md:w-auto"
        />
      </div>
    </div>
  );
};

const Sources = () => {
  const sources = [
    {
      title: 'MCAP',
      description: 'Read robotics and autonomy logs (MCAP format) into Spark DataFrames.',
      icon: FileText,
    },
    {
      title: 'MQTT',
      description: 'Stream IoT telemetry from MQTT brokers as a Spark Structured Streaming source.',
      icon: Radio,
    },
    {
      title: 'ZipDICOM',
      description: 'Decode medical imaging archives (DICOM in ZIP) into queryable pixel data.',
      icon: Image,
    },
  ];

  return (
    <div className="my-6 px-10">
      <h2 className="text-3xl md:text-4xl font-semibold text-center mb-6">
        Available Sources
      </h2>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 w-full">
        {sources.map((source, index) => {
          const Icon = source.icon;
          return (
            <div
              key={index}
              className="bg-white shadow-lg rounded-lg p-6 text-center border border-gray-200 hover:shadow-xl transition-shadow"
            >
              <Icon className="w-8 h-8 mx-auto mb-3 text-red-500" />
              <h3 className="text-lg font-semibold mb-3 text-gray-800">{source.title}</h3>
              <p className="text-gray-600 text-sm">{source.description}</p>
            </div>
          );
        })}
      </div>
    </div>
  );
};

const Capabilities = () => {
  const capabilities = [
    {
      title: 'Spark DataSource V2 API',
      description: 'Implemented against the Python DataSource API — usable from PySpark and Databricks runtime.',
      icon: Database,
    },
    {
      title: 'Batch & Streaming',
      description: 'Both batch reads and Spark Structured Streaming sources are supported where appropriate.',
      icon: Zap,
    },
    {
      title: 'Modular Install',
      description: 'Each connector is an optional extra (mcap, mqtt, zipdcm) — install only what you need.',
      icon: Cog,
    },
  ];

  return (
    <div className="my-6 px-10">
      <h2 className="text-3xl md:text-4xl font-semibold text-center mb-6">
        Capabilities
      </h2>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 w-full">
        {capabilities.map((capability, index) => {
          const Icon = capability.icon;
          return (
            <div
              key={index}
              className="bg-white shadow-lg rounded-lg p-6 text-center border border-gray-200 hover:shadow-xl transition-shadow"
            >
              <Icon className="w-8 h-8 mx-auto mb-3 text-red-500" />
              <h3 className="text-lg font-semibold mb-3 text-gray-800">{capability.title}</h3>
              <p className="text-gray-600 text-sm">{capability.description}</p>
            </div>
          );
        })}
      </div>
    </div>
  );
};

const CallToAction = () => {
  return (
    <div className="flex flex-col justify-center h-screen items-center">
      <h2 className="text-3xl md:text-4xl font-semibold text-center mb-6">
        Plug a new source into Spark 🚀
      </h2>
      <p className="text-center mb-6 text-pretty">
        Follow the installation guide to add Python Data Sources to your PySpark project.
      </p>
      <Button
        variant="primary"
        link="/docs/installation"
        size="large"
        label="Get started ✨"
        className="w-full p-4 font-mono md:w-auto bg-gradient-to-r from-blue-500 to-purple-500 text-white hover:from-blue-600 hover:to-purple-600 transition-all duration-300"
      />
    </div>
  );
};

export default function Home(): JSX.Element {
  return (
    <Layout>
      <main>
        <div className="flex justify-center mx-auto">
          <div className="max-w-screen-lg">
            <Hero />
            <Sources />
            <Capabilities />
            <CallToAction />
          </div>
        </div>
      </main>
    </Layout>
  );
}
